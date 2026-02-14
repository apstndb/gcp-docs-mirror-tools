package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
	"golang.org/x/net/html"
)

const (
	batchSize            = 20
	maxRecursiveDepth    = 5
	maxConsecutiveErrors = 3
)

type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ", ") }
func (s *stringSlice) Set(value string) error {
	for _, v := range strings.Split(value, ",") {
		if v = strings.TrimSpace(v); v != "" {
			*s = append(*s, v)
		}
	}
	return nil
}

type Config struct {
	APIKey         string        `toml:"-"`
	DocsDir        string        `toml:"docs_dir"`
	LogDir         string        `toml:"log_dir"`
	MetadataFile   string        `toml:"metadata_file"`
	Recursive      bool          `toml:"recursive"`
	Refresh        bool          `toml:"refresh"`
	Discovery      bool          `toml:"discovery"`
	Prefixes       []string      `toml:"prefixes"`
	Seeds          []string      `toml:"seeds"`
	QuotaPerMinute float64       `toml:"qpm"`
	QuotaWait      time.Duration `toml:"qw"`
}

func DefaultConfig() *Config {
	return &Config{
		DocsDir:        "docs",
		LogDir:         "logs",
		MetadataFile:   "metadata.yaml",
		Recursive:      false,
		Refresh:        false,
		Discovery:      true, // Default to HTML discovery
		Prefixes:       []string{"/spanner/docs/"},
		QuotaPerMinute: 50.0, // Safer default (half of the 100 QPM hard limit)
		QuotaWait:      70 * time.Second, // Safety buffer for window reset
	}
}

type MirrorApp struct {
	cfg           *Config
	processedURLs map[string]bool
	redirects     map[string]string
	failedURLs    map[string]bool
	mdParser      goldmark.Markdown
	
	// Budget Management
	tokens        float64
	lastRefill    time.Time
}

type Document struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

type APIError struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error"`
}

func main() {
	cfg := DefaultConfig()

	// 1. First pass to find -config flag
	tempFS := flag.NewFlagSet("temp", flag.ContinueOnError)
	tempFS.Usage = func() {} // Silence
	configPath := tempFS.String("config", "", "")
	_ = tempFS.Parse(os.Args[1:])

	// 2. Load TOML if provided (overwrites defaults)
	if *configPath != "" {
		if _, err := toml.DecodeFile(*configPath, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding config: %v\n", err)
			os.Exit(1)
		}
	}

	// 3. Define real flags with current cfg as defaults
	var prefixes stringSlice
	flag.StringVar(&cfg.DocsDir, "docs", cfg.DocsDir, "Output directory for documents")
	flag.StringVar(&cfg.LogDir, "logs", cfg.LogDir, "Directory for log files")
	flag.StringVar(&cfg.MetadataFile, "metadata", cfg.MetadataFile, "Path to metadata summary file")
	flag.BoolVar(&cfg.Recursive, "r", cfg.Recursive, "Recursive discovery from Markdown content")
	flag.BoolVar(&cfg.Refresh, "f", cfg.Refresh, "Refresh existing documents")
	flag.BoolVar(&cfg.Discovery, "discovery", cfg.Discovery, "Discover more links from HTML navigation")
	flag.Float64Var(&cfg.QuotaPerMinute, "qpm", cfg.QuotaPerMinute, "Quota per minute")
	flag.DurationVar(&cfg.QuotaWait, "qw", cfg.QuotaWait, "Wait duration when quota is exceeded")
	flag.Var(&prefixes, "prefix", "Path prefix(es) to mirror")
	
	_ = flag.String("config", *configPath, "Path to TOML configuration file")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] [<seed_url> ...]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if len(prefixes) > 0 {
		cfg.Prefixes = prefixes
	}
	
	seeds := append(cfg.Seeds, flag.Args()...)
	if len(seeds) == 0 && !cfg.Refresh {
		flag.Usage()
		os.Exit(1)
	}

	apiKey := os.Getenv("DEVELOPERKNOWLEDGE_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "Error: DEVELOPERKNOWLEDGE_API_KEY is not set")
		os.Exit(1)
	}
	cfg.APIKey = apiKey

	app := &MirrorApp{
		cfg:           cfg,
		processedURLs: make(map[string]bool),
		redirects:     make(map[string]string),
		failedURLs:    make(map[string]bool),
		mdParser:      goldmark.New(),
		tokens:        cfg.QuotaPerMinute,
		lastRefill:    time.Now(),
	}

	if err := app.Run(seeds); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal Error: %v\n", err)
		os.Exit(1)
	}
}

func (a *MirrorApp) Run(seeds []string) error {
	a.loadMasterListOnly()

	targetURLs := make(map[string]bool)
	for _, s := range seeds {
		targetURLs[s] = true
	}

	if a.cfg.Discovery {
		fmt.Println("--- Step 1: Finding Islands (devsite-tabs-wrapper) ---")
		var islands []string
		for _, s := range seeds {
			found := a.fetchAndExtractLinks(s, []string{"devsite-tabs-wrapper"})
			islands = append(islands, found...)
		}
		islands = deduplicate(islands)
		for _, island := range islands {
			targetURLs[island] = true
		}

		fmt.Println("--- Step 2: Collecting Pages (devsite-nav-list) ---")
		searchRoots := deduplicate(append(seeds, islands...))
		for _, root := range searchRoots {
			pages := a.fetchAndExtractLinks(root, []string{"devsite-nav-list"})
			for _, p := range pages {
				targetURLs[p] = true
			}
		}
	} else {
		fmt.Println("--- Skipping HTML discovery (Direct API Mirroring) ---")
	}

	fmt.Printf("--- Phase 3: API Mirroring (%d unique URLs identified) ---\n", len(targetURLs))
	depth := 1
	for {
		var queue []string
		if depth == 1 {
			for u := range targetURLs {
				if !a.isProcessedSession(u) && a.matchesAnyPrefix(u) {
					queue = append(queue, u)
				}
			}
			if a.cfg.Refresh {
				fmt.Println("--- Refresh Mode: Reloading existing URLs ---")
				for u := range a.processedURLs {
					queue = append(queue, u)
				}
				a.processedURLs = make(map[string]bool)
			}
		}

		if a.cfg.Recursive {
			discoveredFromMD := a.discoverLinksFromMirror()
			queue = append(queue, discoveredFromMD...)
		}
		
		queue = deduplicate(queue)
		if len(queue) == 0 {
			fmt.Println("No more links to process.")
			break
		}

		if err := a.processQueue(depth, queue); err != nil {
			return err
		}

		if !a.cfg.Recursive || depth >= maxRecursiveDepth {
			break
		}
		depth++
	}

	a.saveMetadata()
	return nil
}

func (a *MirrorApp) saveMetadata() {
	if a.cfg.LogDir != "" {
		os.MkdirAll(a.cfg.LogDir, 0755)
	}

	saver := func(filename string, data map[string]bool) {
		if a.cfg.LogDir == "" {
			return
		}
		var l []string
		for k := range data { l = append(l, k) }
		sort.Strings(l)
		os.WriteFile(filepath.Join(a.cfg.LogDir, filename), []byte(strings.Join(l, "\n")+"\n"), 0644)
	}
	
	saver("urls.txt", a.processedURLs)
	saver("failed.txt", a.failedURLs)
	
	var rs []string
	for k, v := range a.redirects { rs = append(rs, k+" "+v) }
	sort.Strings(rs)
	if a.cfg.LogDir != "" {
		os.WriteFile(filepath.Join(a.cfg.LogDir, "redirects.txt"), []byte(strings.Join(rs, "\n")+"\n"), 0644)
	}

	fmt.Println("--- Updating metadata.yaml ---")
	fileCount := 0
	filepath.Walk(a.cfg.DocsDir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Ext(info.Name()) == ".md" {
			fileCount++
		}
		return nil
	})

	metadata := fmt.Sprintf("file_count: %d\nlast_sync: %s\n", 
		fileCount, time.Now().UTC().Format(time.RFC3339))
	os.WriteFile(a.cfg.MetadataFile, []byte(metadata), 0644)
}

func (a *MirrorApp) isProcessedSession(u string) bool {
	return a.processedURLs[u] || a.failedURLs[u] || a.redirects[u] != ""
}

func (a *MirrorApp) takeToken() {
	refillRatePerSec := a.cfg.QuotaPerMinute / 60.0
	now := time.Now()
	elapsed := now.Sub(a.lastRefill).Seconds()
	a.tokens += elapsed * refillRatePerSec
	if a.tokens > a.cfg.QuotaPerMinute {
		a.tokens = a.cfg.QuotaPerMinute
	}
	a.lastRefill = now

	if a.tokens < 1.0 {
		waitTime := time.Duration((1.0 - a.tokens) / refillRatePerSec * float64(time.Second))
		time.Sleep(waitTime)
		a.takeToken()
		return
	}
	a.tokens -= 1.0
}

func (a *MirrorApp) fetchAndExtractLinks(u string, targetClasses []string) []string {
	fmt.Printf("  Scanning HTML: %s\n", u)
	a.takeToken()
	resp, err := http.Get(u)
	if err != nil {
		fmt.Printf("    Warning: %v\n", err)
		return nil
	}
	defer resp.Body.Close()

	basePath := a.toRootRelative(u)
	links := a.extractLinksWithClassFilter(resp.Body, targetClasses)
	
	var results []string
	for _, l := range links {
		normalized := a.resolveAndNormalize(l, basePath)
		if normalized != "" && a.matchesAnyPrefix(normalized) {
			results = append(results, normalized)
		}
	}
	return results
}

func (a *MirrorApp) extractLinksWithClassFilter(r io.Reader, targetClasses []string) []string {
	var links []string
	z := html.NewTokenizer(r)
	depth := 0
	inTargetDepth := 0
	for {
		tt := z.Next()
		switch tt {
		case html.ErrorToken:
			return links
		case html.StartTagToken:
			depth++
			t := z.Token()
			if inTargetDepth == 0 {
				for _, attr := range t.Attr {
					if attr.Key == "class" {
						classes := strings.Fields(attr.Val)
						for _, c := range classes {
							for _, target := range targetClasses {
								if c == target {
									inTargetDepth = depth
									break
								}
							}
						}
					}
				}
			}
			if inTargetDepth > 0 && t.Data == "a" {
				for _, attr := range t.Attr {
					if attr.Key == "href" {
						links = append(links, attr.Val)
					}
				}
			}
		case html.EndTagToken:
			if inTargetDepth == depth {
				inTargetDepth = 0
			}
			depth--
		}
	}
}

func (a *MirrorApp) discoverLinksFromMirror() []string {
	fmt.Println("--- Discovering links from local mirror ---")
	var allDiscovered []string
	filepath.Walk(a.cfg.DocsDir, func(fpath string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(fpath) != ".md" {
			return nil
		}
		relToDocs, _ := filepath.Rel(a.cfg.DocsDir, fpath)
		relToDocs = strings.TrimSuffix(relToDocs, ".md")
		basePath := "/" + strings.TrimPrefix(relToDocs, "docs.cloud.google.com/")
		content, _ := os.ReadFile(fpath)
		links := a.extractLinksFromMarkdown(content)
		for _, l := range links {
			normalized := a.resolveAndNormalize(l, basePath)
			if normalized != "" && !a.isProcessedSession(normalized) && a.matchesAnyPrefix(normalized) {
				allDiscovered = append(allDiscovered, normalized)
			}
		}
		return nil
	})
	return allDiscovered
}

func (a *MirrorApp) resolveAndNormalize(link, basePath string) string {
	if strings.Contains(link, "://") && !strings.Contains(link, "cloud.google.com") {
		return ""
	}
	
	// Handle fragments
	link = strings.Split(link, "#")[0]
	if link == "" {
		return "https://docs.cloud.google.com" + a.toRootRelative(basePath)
	}

	rel := link
	if strings.Contains(rel, "://") {
		rel = a.toRootRelative(rel)
	}
	
	if !strings.HasPrefix(rel, "/") {
		rel = path.Join(basePath, rel)
	}
	return "https://docs.cloud.google.com" + a.toRootRelative(rel)
}

func (a *MirrorApp) matchesAnyPrefix(u string) bool {
	link := a.toRootRelative(u)
	for _, p := range a.cfg.Prefixes {
		cleanP := strings.TrimSuffix(p, "/")
		if link == cleanP || strings.HasPrefix(link, cleanP+"/") {
			return true
		}
	}
	return false
}

func (a *MirrorApp) toRootRelative(u string) string {
	u = strings.TrimPrefix(u, "https://docs.cloud.google.com")
	u = strings.TrimPrefix(u, "https://cloud.google.com")
	u = strings.TrimPrefix(u, "http://docs.cloud.google.com")
	u = strings.TrimPrefix(u, "http://cloud.google.com")
	u = strings.Split(u, "#")[0]
	u = strings.TrimSuffix(u, ".md")
	u = path.Clean("/" + u)
	if u == "." {
		u = "/"
	}
	return u
}

func (a *MirrorApp) extractLinksFromMarkdown(source []byte) []string {
	var links []string
	reader := text.NewReader(source)
	doc := a.mdParser.Parser().Parse(reader)
	ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if entering {
			var dest string
			switch node := n.(type) {
			case *ast.Link:
				dest = string(node.Destination)
			case *ast.AutoLink:
				dest = string(node.URL(source))
			}
			if dest != "" {
				links = append(links, dest)
			}
		}
		return ast.WalkContinue, nil
	})
	return links
}

func (a *MirrorApp) processQueue(depth int, queue []string) error {
	fmt.Printf("--- Pass %d: Processing %d items ---\n", depth, len(queue))
	for i := 0; i < len(queue); i += batchSize {
		end := i + batchSize
		if end > len(queue) {
			end = len(queue)
		}
		if err := a.processBatchRecursive(queue[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (a *MirrorApp) processBatchRecursive(urls []string) error {
	if len(urls) == 0 {
		return nil
	}
	fmt.Printf("Batch fetching %d items... ", len(urls))
	docs, apiErr := a.fetchDocsWithRetry(urls)
	if apiErr == nil {
		fmt.Println("SUCCESS")
		for _, doc := range docs {
			a.saveDoc(doc)
		}
		return nil
	}
	fmt.Printf("FAILED (%s)\n", apiErr.Error.Message)
	if len(urls) == 1 {
		return a.handleLeafFailure(urls[0])
	}
	mid := len(urls) / 2
	a.processBatchRecursive(urls[:mid])
	return a.processBatchRecursive(urls[mid:])
}

func (a *MirrorApp) fetchDocsWithRetry(urls []string) ([]Document, *APIError) {
	for i := 0; i < 5; i++ {
		docs, apiErr := a.fetchDocs(urls)
		if apiErr == nil {
			return docs, nil
		}
		if apiErr.Error.Code == 429 || apiErr.Error.Status == "RESOURCE_EXHAUSTED" {
			fmt.Printf("\n  [Quota Exceeded] Waiting %v for window reset (%d/5)... ", a.cfg.QuotaWait, i+1)
			time.Sleep(a.cfg.QuotaWait)
			a.tokens = a.cfg.QuotaPerMinute
			a.lastRefill = time.Now()
			continue
		}
		return nil, apiErr
	}
	return nil, makeSimpleError("Quota exceeded consistently after retries")
}

func (a *MirrorApp) fetchDocs(urls []string) ([]Document, *APIError) {
	a.takeToken()
	v := url.Values{}
	for _, u := range urls {
		v.Add("names", a.normalizeForAPI(u))
	}
	reqURL := "https://developerknowledge.googleapis.com/v1alpha/documents:batchGet?" + v.Encode()
	req, _ := http.NewRequest("GET", reqURL, nil)
	req.Header.Set("X-Goog-Api-Key", a.cfg.APIKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, makeSimpleError(err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		apiErr := &APIError{}
		apiErr.Error.Code = 429
		apiErr.Error.Message = "Quota exceeded"
		apiErr.Error.Status = "RESOURCE_EXHAUSTED"
		return nil, apiErr
	}

	var res struct {
		Documents []Document `json:"documents"`
		Error     *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, makeSimpleError(fmt.Sprintf("JSON parse error: %v (Status: %d)", err, resp.StatusCode))
	}

	if res.Error != nil {
		apiErr := &APIError{}
		apiErr.Error.Code = res.Error.Code
		apiErr.Error.Message = res.Error.Message
		apiErr.Error.Status = res.Error.Status
		return nil, apiErr
	}
	
	if resp.StatusCode != 200 {
		return nil, makeSimpleError(fmt.Sprintf("HTTP Error %d", resp.StatusCode))
	}

	return res.Documents, nil
}

func (a *MirrorApp) normalizeForAPI(u string) string {
	name := strings.TrimPrefix(u, "https://")
	name = strings.TrimPrefix(name, "http://")
	if strings.HasPrefix(name, "cloud.google.com/") {
		name = "docs.cloud.google.com/" + strings.TrimPrefix(name, "cloud.google.com/")
	}
	return "documents/" + name
}

func (a *MirrorApp) saveDoc(doc Document) {
	relPath := strings.TrimPrefix(doc.Name, "documents/")
	fullPath := filepath.Join(a.cfg.DocsDir, relPath+".md")
	os.MkdirAll(filepath.Dir(fullPath), 0755)
	
	// Normalize trailing newline: Trim trailing whitespace and add exactly one newline.
	content := strings.TrimRight(doc.Content, " \t\r\n") + "\n"
	os.WriteFile(fullPath, []byte(content), 0644)
	
	u := "https://docs.cloud.google.com/" + strings.TrimPrefix(relPath, "docs.cloud.google.com/")
	a.processedURLs[u] = true
}

func (a *MirrorApp) handleLeafFailure(u string) error {
	fmt.Printf("  [Leaf] %s resolve redirect... ", u)
	client := &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
	curr := u
	for i := 0; i < 10; i++ {
		resp, err := client.Get(curr)
		if err != nil || (resp.StatusCode/100 != 3) {
			break
		}
		loc := resp.Header.Get("Location")
		if loc == "" {
			break
		}
		if strings.HasPrefix(loc, "/") {
			loc = "https://docs.cloud.google.com" + loc
		}
		curr = loc
	}
	if curr != u {
		fmt.Printf("found: %s\n", curr)
		a.redirects[u] = curr
	} else {
		fmt.Println("GONE.")
		a.failedURLs[u] = true
		lpath := filepath.Join(a.cfg.DocsDir, strings.TrimPrefix(a.normalizeForAPI(u), "documents/") + ".md")
		os.Remove(lpath)
	}
	return nil
}

func (a *MirrorApp) loadMasterListOnly() {
	if a.cfg.LogDir == "" {
		return
	}
	f, _ := os.Open(filepath.Join(a.cfg.LogDir, "urls.txt"))
	if f == nil {
		return
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		if t := strings.TrimSpace(s.Text()); t != "" {
			a.processedURLs[t] = true
		}
	}
}

func deduplicate(s []string) []string {
	m := make(map[string]bool)
	var res []string
	for _, v := range s {
		if !m[v] { m[v] = true; res = append(res, v) }
	}
	return res
}

func makeSimpleError(msg string) *APIError {
	e := &APIError{}
	e.Error.Message = msg
	return e
}
