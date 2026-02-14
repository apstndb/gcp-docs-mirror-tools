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

func (s *stringSlice) String() string {
	return strings.Join(*s, ", ")
}

func (s *stringSlice) Set(value string) error {
	for _, v := range strings.Split(value, ",") {
		v = strings.TrimSpace(v)
		if v != "" {
			*s = append(*s, v)
		}
	}
	return nil
}

type Config struct {
	APIKey         string
	DocsDir        string
	MasterList     string
	RedirectLog    string
	FailedLog      string
	Recursive      bool
	Refresh        bool
	Prefixes       []string
}

type MirrorApp struct {
	cfg           *Config
	processedURLs map[string]bool
	redirects     map[string]string
	failedURLs    map[string]bool
	errorCount    int
	mdParser      goldmark.Markdown
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
	recursive := flag.Bool("r", false, "Recursive discovery from Markdown content")
	refresh := flag.Bool("f", false, "Refresh existing documents")
	docsDir := flag.String("docs", "docs", "Output directory for documents")
	
	var prefixes stringSlice
	flag.Var(&prefixes, "prefix", "Path prefix(es) to mirror (comma-separated or multiple flags).")
	
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <seed_url> [<seed_url> ...]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	seeds := flag.Args()
	if len(seeds) == 0 && !*refresh {
		flag.Usage()
		os.Exit(1)
	}

	apiKey := os.Getenv("DEVELOPERKNOWLEDGE_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "Error: DEVELOPERKNOWLEDGE_API_KEY is not set")
		os.Exit(1)
	}

	if len(prefixes) == 0 {
		prefixes = []string{"/spanner/docs/"}
	}

	app := &MirrorApp{
		cfg: &Config{
			APIKey:        apiKey,
			DocsDir:       *docsDir,
			MasterList:    "urls.txt",
			RedirectLog:   "redirect_cache.txt",
			FailedLog:     "failed_urls.txt",
			Recursive:     *recursive,
			Refresh:       *refresh,
			Prefixes:      prefixes,
		},
		processedURLs: make(map[string]bool),
		redirects:     make(map[string]string),
		failedURLs:    make(map[string]bool),
		mdParser:      goldmark.New(),
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

	// Phase 3: API Mirroring
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

func (a *MirrorApp) isProcessedSession(u string) bool {
	return a.processedURLs[u] || a.failedURLs[u] || a.redirects[u] != ""
}

func (a *MirrorApp) fetchAndExtractLinks(u string, targetClasses []string) []string {
	fmt.Printf("  Scanning HTML: %s\n", u)
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
	rel := link
	if strings.Contains(rel, "://") {
		rel = a.toRootRelative(rel)
	}
	if !strings.HasPrefix(rel, "/") {
		rel = path.Join(path.Dir(basePath), rel)
	}
	rel = strings.Split(rel, "#")[0]
	return "https://cloud.google.com" + rel
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
	if !strings.HasPrefix(u, "/") {
		u = "/" + u
	}
	return strings.Split(u, "#")[0]
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
	docs, apiErr := a.fetchDocs(urls)
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

func (a *MirrorApp) fetchDocs(urls []string) ([]Document, *APIError) {
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
	var res struct {
		Documents []Document `json:"documents"`
		Error     *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&res)
	if res.Error != nil {
		e := &APIError{}
		e.Error.Code = res.Error.Code
		e.Error.Message = res.Error.Message
		e.Error.Status = res.Error.Status
		return nil, e
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
	os.WriteFile(fullPath, []byte(doc.Content), 0644)
	u := "https://cloud.google.com/" + strings.TrimPrefix(relPath, "docs.cloud.google.com/")
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
	f, _ := os.Open(a.cfg.MasterList)
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

func (a *MirrorApp) saveMetadata() {
	saver := func(p string, data map[string]bool) {
		var l []string
		for k := range data { l = append(l, k) }
		sort.Strings(l)
		os.WriteFile(p, []byte(strings.Join(l, "\n")+"\n"), 0644)
	}
	saver(a.cfg.MasterList, a.processedURLs)
	saver(a.cfg.FailedLog, a.failedURLs)
	var rs []string
	for k, v := range a.redirects { rs = append(rs, k+" "+v) }
	sort.Strings(rs)
	os.WriteFile(a.cfg.RedirectLog, []byte(strings.Join(rs, "\n")+"\n"), 0644)
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
