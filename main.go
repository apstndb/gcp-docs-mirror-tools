package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
	"golang.org/x/net/html"
)

const (
	batchSize            = 20 // Matches Developer Knowledge API limit
	maxRecursiveDepth    = 5
	maxConsecutiveErrors = 3
)

var (
	Version   = "dev"
	Commit    = "unknown"
	BuildTime = "unknown"
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
	Resume         bool          `toml:"resume"`
	Discovery      bool          `toml:"discovery"`
	Verbose        bool          `toml:"verbose"`
	Prefixes       []string      `toml:"prefixes"`
	Seeds          []string      `toml:"seeds"`
	Sitemaps       []string      `toml:"sitemaps"`
	QuotaPerMinute float64       `toml:"qpm"`
	QuotaWait      time.Duration `toml:"qw"`
	SpannerDB      string        `toml:"spanner_db"`
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
		Sitemaps:       nil,
		QuotaPerMinute: 50.0, // Safer default (half of the 100 QPM hard limit)
		QuotaWait:      70 * time.Second, // Safety buffer for window reset
	}
}

type Storage interface {
	Save(docs ...Document) error
	LoadProcessedURLs() (map[string]bool, error)
}

type DiskStorage struct {
	docsDir string
	logDir  string
}

func (s *DiskStorage) Save(docs ...Document) error {
	for _, doc := range docs {
		relPath := strings.TrimPrefix(doc.Name, "documents/")
		fullPath := filepath.Join(s.docsDir, relPath+".md")
		os.MkdirAll(filepath.Dir(fullPath), 0755)

		// Normalize trailing newline: Trim trailing whitespace and add exactly one newline.
		content := strings.TrimRight(doc.Content, " \t\r\n") + "\n"
		if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
			return err
		}
	}
	return nil
}

func (s *DiskStorage) LoadProcessedURLs() (map[string]bool, error) {
	if s.logDir == "" {
		return make(map[string]bool), nil
	}
	f, err := os.Open(filepath.Join(s.logDir, "urls.txt"))
	if err != nil {
		return make(map[string]bool), nil
	}
	defer f.Close()
	processed := make(map[string]bool)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if t := strings.TrimSpace(scanner.Text()); t != "" {
			processed[t] = true
		}
	}
	return processed, nil
}

type MirrorApp struct {
	cfg           *Config
	storage       Storage
	processedURLs map[string]bool
	redirects     map[string]string
	failedURLs    map[string]bool
	mdParser      goldmark.Markdown

	// Shared HTTP clients for Keep-Alive
	httpClient       *http.Client
	noRedirectClient *http.Client

	// Budget & Concurrency Management
	mu         sync.Mutex
	tokens     float64
	lastRefill time.Time
	apiSem     chan struct{}  // Limits concurrent API calls
	redirectWG sync.WaitGroup // Waits for async redirect resolutions

	// Pipelining
	queueChan     chan string
	sessionQueued map[string]bool
	discoveryWG   sync.WaitGroup

	// Progress Tracking
	scannedRawCount int32
	discoveredCount int32
	syncedCount     int32
	skippedCount    int32
	failedCount     int32
	redirectCount   int32
	sitemapTotal    int32
	sitemapDone     int32
	startTime       time.Time
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
	tempFS.Usage = func() {}
	tempFS.SetOutput(io.Discard)
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
	var prefixes, sitemaps stringSlice
	var sitemapFlagProvided bool
	flag.StringVar(&cfg.DocsDir, "docs", cfg.DocsDir, "Output directory for documents")
	flag.StringVar(&cfg.LogDir, "logs", cfg.LogDir, "Directory for log files")
	flag.StringVar(&cfg.MetadataFile, "metadata", cfg.MetadataFile, "Path to metadata summary file")
	flag.BoolVar(&cfg.Recursive, "r", cfg.Recursive, "Recursive discovery from Markdown content")
	flag.BoolVar(&cfg.Refresh, "f", cfg.Refresh, "Refresh existing documents")
	flag.BoolVar(&cfg.Resume, "resume", cfg.Resume, "Resume from existing progress in logs")
	flag.BoolVar(&cfg.Discovery, "discovery", cfg.Discovery, "Discover more links from HTML navigation")
	flag.BoolVar(&cfg.Verbose, "v", cfg.Verbose, "Enable verbose logging")
	flag.Float64Var(&cfg.QuotaPerMinute, "qpm", cfg.QuotaPerMinute, "Quota per minute")
	flag.DurationVar(&cfg.QuotaWait, "qw", cfg.QuotaWait, "Wait duration when quota is exceeded")
	flag.StringVar(&cfg.SpannerDB, "spanner-db", cfg.SpannerDB, "Spanner database for storage (e.g. projects/P/instances/I/databases/D)")
	flag.Var(&prefixes, "prefix", "Path prefix(es) to mirror")
	flag.Func("sitemap", "Sitemap URL(s) to discover links", func(s string) error {
		sitemapFlagProvided = true
		return sitemaps.Set(s)
	})
	
	_ = flag.String("config", *configPath, "Path to TOML configuration file")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] [<seed_url> ...]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if len(prefixes) > 0 {
		cfg.Prefixes = prefixes
	}
	if sitemapFlagProvided {
		cfg.Sitemaps = sitemaps
	}
	
	seeds := append(cfg.Seeds, flag.Args()...)
	if len(seeds) == 0 && !cfg.Refresh && len(cfg.Sitemaps) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	apiKey := os.Getenv("DEVELOPERKNOWLEDGE_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "Error: DEVELOPERKNOWLEDGE_API_KEY is not set")
		os.Exit(1)
	}
	cfg.APIKey = apiKey

	var storage Storage
	if cfg.SpannerDB != "" {
		var err error
		storage, err = NewSpannerStorage(context.Background(), cfg.SpannerDB)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing Spanner: %v\n", err)
			os.Exit(1)
		}
	} else {
		storage = &DiskStorage{docsDir: cfg.DocsDir, logDir: cfg.LogDir}
	}

	app := &MirrorApp{
		cfg:           cfg,
		storage:       storage,
		processedURLs: make(map[string]bool),
		redirects:     make(map[string]string),
		failedURLs:    make(map[string]bool),
		mdParser:      goldmark.New(),
		httpClient:    http.DefaultClient,
		noRedirectClient: &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		tokens:        cfg.QuotaPerMinute,
		lastRefill:    time.Now(),
		apiSem:        make(chan struct{}, 2), // Critical point: max 2 parallel API calls
		queueChan:     make(chan string, 10000),
		sessionQueued: make(map[string]bool),
		startTime:     time.Now(),
	}

	// Start Central Progress Reporter
	stopProgress := make(chan struct{})
	progressDone := make(chan struct{})
	go app.reportProgress(stopProgress, progressDone)

	if err := app.Run(seeds); err != nil {
		app.log("Fatal Error: %v", err)
		os.Exit(1)
	}

	// Wait for any pending async operations (like redirect resolution)
	app.redirectWG.Wait()
	close(stopProgress)
	<-progressDone // Ensure final draw is finished
	
	// Final newline after progress line
	fmt.Println()
}

// log prints a message while handling the progress line correctly
func (a *MirrorApp) log(format string, args ...any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	fmt.Print("\r\033[K") // Clear current line
	fmt.Printf(format+"\n", args...)
	a.drawProgressLocked()
}

// drawProgressLocked draws the progress line. Caller MUST hold a.mu.
func (a *MirrorApp) drawProgressLocked() {
	sDone := atomic.LoadInt32(&a.sitemapDone)
	sTotal := atomic.LoadInt32(&a.sitemapTotal)
	raw := atomic.LoadInt32(&a.scannedRawCount)
	disc := atomic.LoadInt32(&a.discoveredCount)
	synced := atomic.LoadInt32(&a.syncedCount)
	fail := atomic.LoadInt32(&a.failedCount)
	redir := atomic.LoadInt32(&a.redirectCount)
	skip := atomic.LoadInt32(&a.skippedCount)

	done := synced + fail + redir + skip
	percent := 0.0
	if disc > 0 {
		percent = float64(done) / float64(disc) * 100
	}

	elapsed := time.Since(a.startTime).Seconds()
	rate := 0.0
	if elapsed > 0 {
		rate = float64(done) / elapsed
	}

	// Calculate ETA
	eta := "??:??"
	if rate > 0 && disc > done {
		remaining := float64(disc-done) / rate
		d := time.Duration(remaining) * time.Second
		eta = fmt.Sprintf("%02d:%02d", int(d.Minutes()), int(d.Seconds())%60)
	}

	// Calculate current tokens (with refill) for display
	refillRatePerSec := a.cfg.QuotaPerMinute / 60.0
	now := time.Now()
	currentTokens := a.tokens + now.Sub(a.lastRefill).Seconds()*refillRatePerSec
	if currentTokens > a.cfg.QuotaPerMinute {
		currentTokens = a.cfg.QuotaPerMinute
	}
	if currentTokens < 0 {
		currentTokens = 0
	}

	fmt.Printf("\r[Sitemaps: %d/%d] [Scan: %d] [Total: %d] [Done: %d (%.1f%%)] [Synced: %d] [Skip: %d] [Redir: %d] [Fail: %d] [Rate: %.1f/s] [ETA: %s] [Quota: %.1f]   ",
		sDone, sTotal, raw, disc, done, percent, synced, skip, redir, fail, rate, eta, currentTokens)
}

func (a *MirrorApp) reportProgress(stop <-chan struct{}, done chan<- struct{}) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	defer close(done)

	for {
		select {
		case <-stop:
			// Final draw
			a.mu.Lock()
			a.drawProgressLocked()
			a.mu.Unlock()
			return
		case <-ticker.C:
			a.mu.Lock()
			a.drawProgressLocked()
			a.mu.Unlock()
		}
	}
}

func (a *MirrorApp) Run(seeds []string) error {
	if a.cfg.Resume {
		a.loadMasterListOnly()
	}

	// shared work counter to track all enqueued tasks including recursive ones
	var activeWork sync.WaitGroup

	// Start URL Processor (Consumer)
	processDone := make(chan error, 1)
	go func() {
		processDone <- a.processStream(&activeWork)
	}()

	// Discovery 0: Sitemaps
	if len(a.cfg.Sitemaps) > 0 {
		a.discoveryWG.Go(func() {
			a.DiscoverFromSitemaps(a.cfg.Sitemaps, &activeWork)
		})
	}

	// Discovery 1: Seeds
	for _, s := range seeds {
		a.enqueue(s, &activeWork)
	}

	// Discovery 2: HTML Scan (Islands & Nav List)
	if a.cfg.Discovery {
		a.discoveryWG.Go(func() {
			var islands []string
			for _, s := range seeds {
				found := a.fetchAndExtractLinks(s, []string{"devsite-tabs-wrapper"})
				islands = append(islands, found...)
			}
			slices.Sort(islands)
			islands = slices.Compact(islands)
			for _, island := range islands {
				a.enqueue(island, &activeWork)
			}

			searchRoots := append(seeds, islands...)
			slices.Sort(searchRoots)
			searchRoots = slices.Compact(searchRoots)
			for _, root := range searchRoots {
				pages := a.fetchAndExtractLinks(root, []string{"devsite-nav-list"})
				for _, p := range pages {
					a.enqueue(p, &activeWork)
				}
			}
		})
	}

	// Discovery 3: Local Mirror (Recursive)
	if a.cfg.Recursive {
		a.discoveryWG.Go(func() {
			discoveredFromMD := a.discoverLinksFromMirror()
			a.enqueueBatch(discoveredFromMD, &activeWork)
		})
	}

	// Discovery 4: Refresh Mode
	if a.cfg.Refresh {
		a.log("Refresh Mode: Reloading existing URLs")
		a.mu.Lock()
		existing := make([]string, 0, len(a.processedURLs))
		for u := range a.processedURLs {
			existing = append(existing, u)
		}
		a.mu.Unlock()
		a.enqueueBatch(existing, &activeWork)
	}

	// Close channel when all work (initial + recursive/redirects) finish
	go func() {
		a.discoveryWG.Wait()
		activeWork.Wait()
		close(a.queueChan)
	}()

	err := <-processDone
	a.saveMetadata()
	return err
}

func (a *MirrorApp) enqueue(u string, wg *sync.WaitGroup) {
	a.enqueueBatch([]string{u}, wg)
}

func (a *MirrorApp) enqueueBatch(urls []string, wg *sync.WaitGroup) {
	a.mu.Lock()
	defer a.mu.Unlock()
	
	var discovered int32
	var skipped int32
	for _, u := range urls {
		if !a.matchesAnyPrefix(u) {
			continue
		}
		
		if a.sessionQueued[u] {
			continue
		}
		a.sessionQueued[u] = true
		discovered++

		if !a.cfg.Refresh && (a.processedURLs[u] || a.failedURLs[u] || a.redirects[u] != "") {
			skipped++
			continue
		}
		
		wg.Add(1) // Increment work counter for each URL actually enqueued
		a.queueChan <- u
	}
	atomic.AddInt32(&a.discoveredCount, discovered)
	atomic.AddInt32(&a.skippedCount, skipped)
}

func (a *MirrorApp) processStream(wg *sync.WaitGroup) error {
	a.log("Phase 3: Pipelined API Mirroring")
	
	type batch []string
	batches := make(chan batch)
	var workerWG sync.WaitGroup
	numWorkers := 30 

	var firstErr error
	var errMu sync.Mutex

	// Start Workers
	for w := 0; w < numWorkers; w++ {
		workerWG.Go(func() {
			for b := range batches {
				err := a.processBatchRecursive(b, wg)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
				}
			}
		})
	}

	// Collector: batch items from queueChan
	var currentBatch []string
	for u := range a.queueChan {
		currentBatch = append(currentBatch, u)
		if len(currentBatch) >= batchSize {
			batches <- currentBatch
			currentBatch = nil
		}
	}
	if len(currentBatch) > 0 {
		batches <- currentBatch
	}
	close(batches)
	workerWG.Wait()
	a.log("Mirroring complete.")

	return firstErr
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
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.processedURLs[u] || a.failedURLs[u] || a.redirects[u] != ""
}

func (a *MirrorApp) takeToken() {
	for {
		a.mu.Lock()
		refillRatePerSec := a.cfg.QuotaPerMinute / 60.0
		now := time.Now()
		elapsed := now.Sub(a.lastRefill).Seconds()
		a.tokens += elapsed * refillRatePerSec
		if a.tokens > a.cfg.QuotaPerMinute {
			a.tokens = a.cfg.QuotaPerMinute
		}
		a.lastRefill = now

		if a.tokens >= 1.0 {
			a.tokens -= 1.0
			a.mu.Unlock()
			return
		}

		waitTime := time.Duration((1.0 - a.tokens) / refillRatePerSec * float64(time.Second))
		a.mu.Unlock()
		time.Sleep(waitTime)
	}
}

func (a *MirrorApp) fetchAndExtractLinks(u string, targetClasses []string) []string {
	resp, err := a.httpClient.Get(u)
	if err != nil {
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
	u = strings.Split(u, "?")[0]
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

func (a *MirrorApp) processBatchRecursive(urls []string, wg *sync.WaitGroup) error {
	if len(urls) == 0 {
		return nil
	}
	docs, apiErr := a.fetchDocsWithRetry(urls)
	if apiErr == nil {
		// Sync save to storage
		if err := a.storage.Save(docs...); err != nil {
			a.log("Storage error: %v", err)
		}
		
		a.mu.Lock()
		processedMap := make(map[string]bool)
		for _, doc := range docs {
			relPath := strings.TrimPrefix(doc.Name, "documents/")
			u := "https://docs.cloud.google.com/" + strings.TrimPrefix(relPath, "docs.cloud.google.com/")
			a.processedURLs[u] = true
			processedMap[u] = true
		}
		atomic.AddInt32(&a.syncedCount, int32(len(docs)))
		
		// Handle potential silent failures (docs requested but not returned)
		for _, u := range urls {
			if !processedMap[u] {
				// Treat missing docs as failures to keep progress accurate
				a.failedURLs[u] = true
				atomic.AddInt32(&a.failedCount, 1)
			}
			wg.Done() // URL processing finished (synced or missing)
		}
		a.mu.Unlock()
		return nil
	}
	
	if len(urls) == 1 {
		// Offload redirect resolution to separate goroutine to not block API pipeline
		a.redirectWG.Add(1)
		go func(u string) {
			defer a.redirectWG.Done()
			a.handleLeafFailure(u, wg)
		}(urls[0])
		return nil
	}
	mid := len(urls) / 2
	a.processBatchRecursive(urls[:mid], wg)
	return a.processBatchRecursive(urls[mid:], wg)
}

func (a *MirrorApp) fetchDocsWithRetry(urls []string) ([]Document, *APIError) {
	for i := 0; i < 5; i++ {
		docs, apiErr := a.fetchDocs(urls)
		if apiErr == nil {
			return docs, nil
		}
		if apiErr.Error.Code == 429 || apiErr.Error.Status == "RESOURCE_EXHAUSTED" {
			time.Sleep(a.cfg.QuotaWait)
			a.mu.Lock()
			a.tokens = a.cfg.QuotaPerMinute
			a.lastRefill = time.Now()
			a.mu.Unlock()
			continue
		}
		return nil, apiErr
	}
	return nil, makeSimpleError("Quota exceeded consistently after retries")
}

func (a *MirrorApp) fetchDocs(urls []string) ([]Document, *APIError) {
	// Limit concurrent API calls to avoid overwhelming the strict quota
	a.apiSem <- struct{}{}
	defer func() { <-a.apiSem }()

	a.takeToken()
	v := url.Values{}
	for _, u := range urls {
		v.Add("names", a.normalizeForAPI(u))
	}
	reqURL := "https://developerknowledge.googleapis.com/v1alpha/documents:batchGet?" + v.Encode()
	req, _ := http.NewRequest("GET", reqURL, nil)
	req.Header.Set("X-Goog-Api-Key", a.cfg.APIKey)

	resp, err := a.httpClient.Do(req)
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

func (a *MirrorApp) handleLeafFailure(u string, wg *sync.WaitGroup) error {
	curr := u
	for i := 0; i < 10; i++ {
		resp, err := a.noRedirectClient.Get(curr)
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
		a.mu.Lock()
		a.redirects[u] = curr
		atomic.AddInt32(&a.redirectCount, 1)
		a.mu.Unlock()
		a.enqueue(curr, wg)
	} else {
		a.mu.Lock()
		a.failedURLs[u] = true
		atomic.AddInt32(&a.failedCount, 1)
		a.mu.Unlock()
		lpath := filepath.Join(a.cfg.DocsDir, strings.TrimPrefix(a.normalizeForAPI(u), "documents/") + ".md")
		os.Remove(lpath)
	}
	wg.Done() // Original URL processing (which failed/redirected) is finished
	return nil
}

func (a *MirrorApp) loadMasterListOnly() {
	processed, _ := a.storage.LoadProcessedURLs()
	a.mu.Lock()
	for k, v := range processed {
		a.processedURLs[k] = v
	}
	a.mu.Unlock()
}

func makeSimpleError(msg string) *APIError {
	e := &APIError{}
	e.Error.Message = msg
	return e
}
