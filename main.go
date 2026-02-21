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
	"golang.org/x/time/rate"
)

const (
	batchSize            = 20 // Matches Developer Knowledge API limit
	maxRecursiveDepth    = 5
	maxConsecutiveErrors = 3
	diagnosticInterval   = 30 * time.Second
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
	StallTimeout   time.Duration `toml:"stall_timeout"`
}

func DefaultConfig() *Config {
	return &Config{
		DocsDir:        "docs",
		LogDir:         "logs",
		MetadataFile:   "metadata.yaml",
		Recursive:      false,
		Refresh:        false,
		Discovery:      true,
		Prefixes:       []string{"/firestore/docs/"},
		Sitemaps:       nil,
		QuotaPerMinute: 50.0,
		QuotaWait:      70 * time.Second,
		StallTimeout:   0,
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
		// Normalize trailing newline
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
	failedURLs    map[string]int // URL -> HTTP StatusCode
	mdParser      goldmark.Markdown

	// Shared HTTP clients
	httpClient       *http.Client
	noRedirectClient *http.Client

	// Budget & Concurrency Management
	mu         sync.Mutex
	limiter    *rate.Limiter
	apiSem     chan struct{}
	httpSem    chan struct{} // Limits concurrent HTTP checks
	redirectWG sync.WaitGroup

	// Pipelining
	queueChan     chan string
	sessionQueued map[string]bool
	discoveryWG   sync.WaitGroup

	// Progress Tracking
	scannedRawCount int32
	discoveredCount int32
	finishedCount   int32
	syncedCount     int32
	skippedCount    int32
	failedCount     int32
	redirectCount   int32
	sitemapTotal    int32
	sitemapDone     int32
	
	apiReqCount     int32
	httpReqCount    int32
	inflightCount   int32
	activeDiscovery int32
	isWaitingQuota  int32
	
	apiWindow  [60]int32
	httpWindow [60]int32
	lastWindowUpdate int64

	lastActivity    int64
	startTime       time.Time
	isCI            bool
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
	tempFS := flag.NewFlagSet("temp", flag.ContinueOnError)
	tempFS.Usage = func() {}
	tempFS.SetOutput(io.Discard)
	configPath := tempFS.String("config", "", "")
	_ = tempFS.Parse(os.Args[1:])

	if *configPath != "" {
		if _, err := toml.DecodeFile(*configPath, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding config: %v\n", err)
			os.Exit(1)
		}
	}

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
	flag.DurationVar(&cfg.StallTimeout, "stall-timeout", cfg.StallTimeout, "Max duration without activity before aborting")
	flag.StringVar(&cfg.SpannerDB, "spanner-db", cfg.SpannerDB, "Spanner database for storage")
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

	if len(prefixes) > 0 { cfg.Prefixes = prefixes }
	if sitemapFlagProvided { cfg.Sitemaps = sitemaps }
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
		failedURLs:    make(map[string]int),
		mdParser:      goldmark.New(),
		httpClient:    &http.Client{Timeout: 30 * time.Second},
		noRedirectClient: &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		limiter:       rate.NewLimiter(rate.Limit(cfg.QuotaPerMinute/60.0), int(cfg.QuotaPerMinute)),
		apiSem:        make(chan struct{}, 8), // Increased concurrency
		httpSem:       make(chan struct{}, 20), // Limit concurrent HTTP checks
		queueChan:     make(chan string, 10000),
		sessionQueued: make(map[string]bool),
		startTime:     time.Now(),
		lastActivity:  time.Now().UnixNano(),
		lastWindowUpdate: time.Now().Unix(),
		isCI:          os.Getenv("CI") == "true",
	}

	stopProgress := make(chan struct{})
	progressDone := make(chan struct{})
	go app.reportProgress(stopProgress, progressDone)

	if err := app.Run(seeds); err != nil {
		app.log("Fatal Error: %v", err)
		os.Exit(1)
	}

	app.redirectWG.Wait()
	close(stopProgress)
	<-progressDone
	fmt.Println()
}

func (a *MirrorApp) markActivity() {
	atomic.StoreInt64(&a.lastActivity, time.Now().UnixNano())
}

func (a *MirrorApp) recordAPIRequest() {
	atomic.AddInt32(&a.apiReqCount, 1)
	now := time.Now().Unix()
	idx := now % 60
	for {
		last := atomic.LoadInt64(&a.lastWindowUpdate)
		if last == now { break }
		if atomic.CompareAndSwapInt64(&a.lastWindowUpdate, last, now) {
			atomic.StoreInt32(&a.apiWindow[idx], 0)
			atomic.StoreInt32(&a.httpWindow[idx], 0)
			break
		}
	}
	atomic.AddInt32(&a.apiWindow[idx], 1)
}

func (a *MirrorApp) recordHTTPRequest() {
	atomic.AddInt32(&a.httpReqCount, 1)
	now := time.Now().Unix()
	idx := now % 60
	for {
		last := atomic.LoadInt64(&a.lastWindowUpdate)
		if last == now { break }
		if atomic.CompareAndSwapInt64(&a.lastWindowUpdate, last, now) {
			atomic.StoreInt32(&a.apiWindow[idx], 0)
			atomic.StoreInt32(&a.httpWindow[idx], 0)
			break
		}
	}
	atomic.AddInt32(&a.httpWindow[idx], 1)
}

func (a *MirrorApp) getWindowedQPM() (float64, float64) {
	var api, http int32
	for i := 0; i < 60; i++ {
		api += atomic.LoadInt32(&a.apiWindow[i])
		http += atomic.LoadInt32(&a.httpWindow[i])
	}
	return float64(api), float64(http)
}

func (a *MirrorApp) log(format string, args ...any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.isCI { fmt.Print("\r\033[K") }
	fmt.Printf(format+"\n", args...)
	if !a.isCI { a.drawProgressLocked() }
}

func (a *MirrorApp) drawProgressLocked() {
	sDone := atomic.LoadInt32(&a.sitemapDone)
	sTotal := atomic.LoadInt32(&a.sitemapTotal)
	raw := atomic.LoadInt32(&a.scannedRawCount)
	disc := atomic.LoadInt32(&a.discoveredCount)
	synced := atomic.LoadInt32(&a.syncedCount)
	fail := atomic.LoadInt32(&a.failedCount)
	redir := atomic.LoadInt32(&a.redirectCount)
	skip := atomic.LoadInt32(&a.skippedCount)
	done := atomic.LoadInt32(&a.finishedCount)
	inflight := atomic.LoadInt32(&a.inflightCount)
	waiting := atomic.LoadInt32(&a.isWaitingQuota) != 0
	apiQPM, httpQPM := a.getWindowedQPM()

	percent := 0.0
	if disc > 0 { percent = float64(done) / float64(disc) * 100 }

	elapsedSecs := time.Since(a.startTime).Seconds()
	overallRate := 0.0
	if elapsedSecs > 0 { overallRate = float64(done) / elapsedSecs }

	eta := "??:??"
	if overallRate > 0 && disc > done {
		remaining := float64(disc-done) / overallRate
		d := time.Duration(remaining) * time.Second
		eta = fmt.Sprintf("%02d:%02d", int(d.Minutes()), int(d.Seconds())%60)
	}

	status := ""
	if waiting { status = "[WAITING QUOTA] " }

	if a.isCI {
		fmt.Printf("[%s] %s[Progress] Sitemaps:%d/%d Scan:%d Total:%d Done:%d (%.1f%%) Inflight:%d Synced:%d Skip:%d Redir:%d Fail:%d API_QPM:%.1f HTTP_QPM:%.1f Rate:%.1f/s ETA:%s\n",
			time.Now().Format("15:04:05"), status, sDone, sTotal, raw, disc, done, percent, inflight, synced, skip, redir, fail, apiQPM, httpQPM, overallRate, eta)
	} else {
		fmt.Printf("\r%s[Sitemaps: %d/%d] [Scan: %d] [Total: %d] [Done: %d (%.1f%%)] [Inflight: %d] [API_QPM: %.1f] [HTTP_QPM: %.1f] [Rate: %.1f/s] [ETA: %s]   ",
			status, sDone, sTotal, raw, disc, done, percent, inflight, apiQPM, httpQPM, overallRate, eta)
	}
}

func (a *MirrorApp) reportProgress(stop <-chan struct{}, done chan<- struct{}) {
	interval := 500 * time.Millisecond
	if a.isCI { interval = 10 * time.Second }
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(done)

	for {
		select {
		case <-stop:
			a.mu.Lock()
			a.drawProgressLocked()
			a.mu.Unlock()
			return
		case <-ticker.C:
			a.mu.Lock()
			a.drawProgressLocked()
			last := atomic.LoadInt64(&a.lastActivity)
			idle := time.Since(time.Unix(0, last))
			if idle > diagnosticInterval {
				inflight := atomic.LoadInt32(&a.inflightCount)
				discWG := atomic.LoadInt32(&a.activeDiscovery)
				a.mu.Unlock()
				a.log("[DIAGNOSTIC] Long idle detected (%v). Inflight: %d, Queue: %d, DiscoveryWG: %d", 
					idle.Round(time.Second), inflight, len(a.queueChan), discWG)
				a.mu.Lock()
			}
			if a.cfg.StallTimeout > 0 && idle > a.cfg.StallTimeout {
				a.mu.Unlock()
				fmt.Fprintf(os.Stderr, "\n[FATAL] Activity stall timeout exceeded (%v). Aborting.\n", a.cfg.StallTimeout)
				os.Exit(1)
			}
			a.mu.Unlock()
		}
	}
}

func (a *MirrorApp) Run(seeds []string) error {
	a.log("GCP Docs Mirror Tool %s (%s) built at %s", Version, Commit, BuildTime)
	a.log("Starting mirror process...")
	a.log("  - Seeds:    %d", len(seeds))
	a.log("  - Prefixes: %v", a.cfg.Prefixes)
	a.log("  - Quota:    %.1f QPM", a.cfg.QuotaPerMinute)
	if a.cfg.SpannerDB != "" { a.log("  - Storage:  Spanner (%s)", a.cfg.SpannerDB) } else { a.log("  - Storage:  Disk (%s)", a.cfg.DocsDir) }

	if a.cfg.Resume { a.loadMasterListOnly() }
	var activeWork sync.WaitGroup
	processDone := make(chan error, 1)
	go func() { processDone <- a.processStream(&activeWork) }()

	if len(a.cfg.Sitemaps) > 0 {
		atomic.AddInt32(&a.activeDiscovery, 1)
		a.discoveryWG.Add(1)
		go func() {
			defer a.discoveryWG.Done()
			defer atomic.AddInt32(&a.activeDiscovery, -1)
			a.DiscoverFromSitemaps(a.cfg.Sitemaps, &activeWork)
		}()
	}

	a.enqueueBatch(seeds, &activeWork)

	if a.cfg.Discovery {
		atomic.AddInt32(&a.activeDiscovery, 1)
		a.discoveryWG.Add(1)
		go func() {
			defer a.discoveryWG.Done()
			defer atomic.AddInt32(&a.activeDiscovery, -1)
			var islands []string
			for _, s := range seeds { islands = append(islands, a.fetchAndExtractLinks(s, []string{"devsite-tabs-wrapper"})...) }
			slices.Sort(islands)
			islands = slices.Compact(islands)
			a.enqueueBatch(islands, &activeWork)
			
			searchRoots := append(seeds, islands...)
			slices.Sort(searchRoots)
			searchRoots = slices.Compact(searchRoots)
			for _, root := range searchRoots {
				found := a.fetchAndExtractLinks(root, []string{"devsite-nav-list"})
				a.enqueueBatch(found, &activeWork)
			}
		}()
	}

	if a.cfg.Recursive {
		atomic.AddInt32(&a.activeDiscovery, 1)
		a.discoveryWG.Add(1)
		go func() {
			defer a.discoveryWG.Done()
			defer atomic.AddInt32(&a.activeDiscovery, -1)
			a.enqueueBatch(a.discoverLinksFromMirror(), &activeWork)
		}()
	}

	if a.cfg.Refresh {
		a.mu.Lock()
		existing := make([]string, 0, len(a.processedURLs))
		for u := range a.processedURLs { existing = append(existing, u) }
		a.mu.Unlock()
		a.enqueueBatch(existing, &activeWork)
	}

	stopSaver := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C: a.saveMetadata()
			case <-stopSaver: return
			}
		}
	}()

	go func() {
		a.discoveryWG.Wait()
		activeWork.Wait()
		close(a.queueChan)
	}()

	err := <-processDone
	close(stopSaver)
	a.saveMetadata()
	return err
}

func (a *MirrorApp) enqueue(u string, wg *sync.WaitGroup) {
	a.enqueueBatch([]string{u}, wg)
}

func (a *MirrorApp) enqueueBatch(urls []string, wg *sync.WaitGroup) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var discovered, skipped int32
	for _, u := range urls {
		if !a.matchesAnyPrefix(u) { continue }
		if a.sessionQueued[u] { continue }
		a.sessionQueued[u] = true
		discovered++
		if !a.cfg.Refresh && (a.processedURLs[u] || a.failedURLs[u] != 0 || a.redirects[u] != "") {
			atomic.AddInt32(&a.finishedCount, 1)
			skipped++
			continue
		}
		atomic.AddInt32(&a.inflightCount, 1)
		wg.Add(1)
		a.queueChan <- u
		a.markActivity()
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

	for w := 0; w < numWorkers; w++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for b := range batches {
				if err := a.processBatchRecursive(b, wg); err != nil {
					errMu.Lock()
					if firstErr == nil { firstErr = err }
					errMu.Unlock()
				}
			}
		}()
	}

	var currentBatch []string
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	flush := func() {
		if len(currentBatch) > 0 {
			batches <- currentBatch
			currentBatch = nil
		}
	}

	for {
		select {
		case u, ok := <-a.queueChan:
			if !ok {
				flush()
				close(batches)
				workerWG.Wait()
				return firstErr
			}
			currentBatch = append(currentBatch, u)
			if len(currentBatch) >= batchSize { flush() }
		case <-ticker.C: flush()
		}
	}
}

func (a *MirrorApp) saveMetadata() {
	if a.cfg.LogDir != "" { os.MkdirAll(a.cfg.LogDir, 0755) }
	
	a.mu.Lock()
	var urls []string
	for k := range a.processedURLs { urls = append(urls, k) }
	sort.Strings(urls)
	
	var fails []string
	for k, v := range a.failedURLs { fails = append(fails, fmt.Sprintf("%d %s", v, k)) }
	sort.Strings(fails)

	var rs []string
	for k, v := range a.redirects { rs = append(rs, k+" "+v) }
	sort.Strings(rs)
	a.mu.Unlock()

	if a.cfg.LogDir != "" {
		os.WriteFile(filepath.Join(a.cfg.LogDir, "urls.txt"), []byte(strings.Join(urls, "\n")+"\n"), 0644)
		os.WriteFile(filepath.Join(a.cfg.LogDir, "failed.txt"), []byte(strings.Join(fails, "\n")+"\n"), 0644)
		os.WriteFile(filepath.Join(a.cfg.LogDir, "redirects.txt"), []byte(strings.Join(rs, "\n")+"\n"), 0644)
	}

	fileCount := 0
	filepath.Walk(a.cfg.DocsDir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Ext(info.Name()) == ".md" { fileCount++ }
		return nil
	})
	metadata := fmt.Sprintf("file_count: %d\nlast_sync: %s\n", fileCount, time.Now().UTC().Format(time.RFC3339))
	os.WriteFile(a.cfg.MetadataFile, []byte(metadata), 0644)
}

func (a *MirrorApp) isProcessedSession(u string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.processedURLs[u] || a.failedURLs[u] != 0 || a.redirects[u] != ""
}

func (a *MirrorApp) takeTokens(n int) {
	atomic.StoreInt32(&a.isWaitingQuota, 1)
	a.markActivity()
	_ = a.limiter.WaitN(context.Background(), n)
	atomic.StoreInt32(&a.isWaitingQuota, 0)
}

func (a *MirrorApp) fetchAndExtractLinks(u string, targetClasses []string) []string {
	a.recordHTTPRequest()
	resp, err := a.httpClient.Get(u)
	if err != nil { return nil }
	defer resp.Body.Close()
	actualURL := resp.Request.URL.String()
	basePath := a.toRootRelative(actualURL)
	var results []string
	for _, l := range a.extractLinksWithClassFilter(resp.Body, targetClasses) {
		if normalized := a.resolveAndNormalize(l, basePath); normalized != "" && a.matchesAnyPrefix(normalized) {
			results = append(results, normalized)
		}
	}
	a.markActivity()
	return results
}

func (a *MirrorApp) extractLinksWithClassFilter(r io.Reader, targetClasses []string) []string {
	var links []string
	z := html.NewTokenizer(r)
	depth, inTargetDepth := 0, 0
	for {
		tt := z.Next()
		switch tt {
		case html.ErrorToken: return links
		case html.StartTagToken:
			depth++
			t := z.Token()
			if inTargetDepth == 0 {
				for _, attr := range t.Attr {
					if attr.Key == "class" {
						for _, c := range strings.Fields(attr.Val) {
							for _, target := range targetClasses {
								if c == target { inTargetDepth = depth; break }
							}
						}
					}
				}
			}
			if inTargetDepth > 0 && t.Data == "a" {
				for _, attr := range t.Attr {
					if attr.Key == "href" { links = append(links, attr.Val) }
				}
			}
		case html.EndTagToken:
			if inTargetDepth == depth { inTargetDepth = 0 }
			depth--
		}
	}
}

func (a *MirrorApp) discoverLinksFromMirror() []string {
	var allDiscovered []string
	filepath.Walk(a.cfg.DocsDir, func(fpath string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(fpath) != ".md" { return nil }
		relToDocs, _ := filepath.Rel(a.cfg.DocsDir, fpath)
		relToDocs = strings.TrimSuffix(relToDocs, ".md")
		basePath := "/" + strings.TrimPrefix(relToDocs, "docs.cloud.google.com/")
		content, _ := os.ReadFile(fpath)
		for _, l := range a.extractLinksFromMarkdown(content) {
			if normalized := a.resolveAndNormalize(l, basePath); normalized != "" && !a.isProcessedSession(normalized) && a.matchesAnyPrefix(normalized) {
				allDiscovered = append(allDiscovered, normalized)
			}
		}
		return nil
	})
	return allDiscovered
}

func (a *MirrorApp) resolveAndNormalize(link, basePath string) string {
	if strings.Contains(link, "://") && !strings.Contains(link, "cloud.google.com") { return "" }
	link = strings.Split(link, "#")[0]
	if link == "" { return "https://docs.cloud.google.com" + a.toRootRelative(basePath) }
	rel := link
	if strings.Contains(rel, "://") { rel = a.toRootRelative(rel) }
	if !strings.HasPrefix(rel, "/") { rel = path.Join(basePath, rel) }
	return "https://docs.cloud.google.com" + a.toRootRelative(rel)
}

func (a *MirrorApp) matchesAnyPrefix(u string) bool {
	link := a.toRootRelative(u)
	for _, p := range a.cfg.Prefixes {
		cleanP := strings.TrimSuffix(p, "/")
		if link == cleanP || strings.HasPrefix(link, cleanP+"/") { return true }
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
	if u == "." { u = "/" }
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
			case *ast.Link: dest = string(node.Destination)
			case *ast.AutoLink: dest = string(node.URL(source))
			}
			if dest != "" { links = append(links, dest) }
		}
		return ast.WalkContinue, nil
	})
	return links
}

func (a *MirrorApp) processBatchRecursive(urls []string, wg *sync.WaitGroup) error {
	if len(urls) == 0 { return nil }
	docs, apiErr := a.fetchDocsWithRetry(urls)
	if apiErr == nil {
		if err := a.storage.Save(docs...); err != nil { a.log("Storage error: %v", err) }
		a.mu.Lock()
		processedMap := make(map[string]bool)
		for _, doc := range docs {
			relPath := strings.TrimPrefix(doc.Name, "documents/")
			u := "https://docs.cloud.google.com/" + strings.TrimPrefix(relPath, "docs.cloud.google.com/")
			a.processedURLs[u] = true
			processedMap[u] = true
		}
		atomic.AddInt32(&a.syncedCount, int32(len(docs)))
		
		for _, u := range urls {
			if !processedMap[u] {
				// Re-check silent API failures via HTTP for redirects
				a.redirectWG.Add(1)
				go func(url string) {
					defer a.redirectWG.Done()
					a.handleLeafFailure(url, wg)
				}(u)
				continue
			}
			atomic.AddInt32(&a.inflightCount, -1)
			atomic.AddInt32(&a.finishedCount, 1)
			wg.Done()
		}
		a.markActivity()
		a.mu.Unlock()
		return nil
	}
	if len(urls) == 1 {
		a.redirectWG.Add(1)
		go func(u string) {
			defer a.redirectWG.Done()
			a.handleLeafFailure(u, wg)
		}(urls[0])
		return nil
	}
	mid := len(urls) / 2
	go a.processBatchRecursive(urls[:mid], wg)
	go a.processBatchRecursive(urls[mid:], wg)
	return nil
}

func (a *MirrorApp) fetchDocsWithRetry(urls []string) ([]Document, *APIError) {
	for i := 0; i < 5; i++ {
		docs, apiErr := a.fetchDocs(urls)
		if apiErr == nil { return docs, nil }
		if apiErr.Error.Code == 429 || apiErr.Error.Status == "RESOURCE_EXHAUSTED" {
			a.log("Quota exceeded (429). Waiting %v for window reset (attempt %d/5)...", a.cfg.QuotaWait, i+1)
			atomic.StoreInt32(&a.isWaitingQuota, 1)
			a.markActivity()
			time.Sleep(a.cfg.QuotaWait)
			atomic.StoreInt32(&a.isWaitingQuota, 0)
			continue
		}
		return nil, apiErr
	}
	return nil, makeSimpleError("Quota exceeded consistently after retries")
}

func (a *MirrorApp) fetchDocs(urls []string) ([]Document, *APIError) {
	a.apiSem <- struct{}{}
	defer func() { <-a.apiSem }()
	a.recordAPIRequest()
	a.takeTokens(1)
	v := url.Values{}
	for _, u := range urls { v.Add("names", a.normalizeForAPI(u)) }
	reqURL := "https://developerknowledge.googleapis.com/v1alpha/documents:batchGet?" + v.Encode()
	req, _ := http.NewRequest("GET", reqURL, nil)
	req.Header.Set("X-Goog-Api-Key", a.cfg.APIKey)
	resp, err := a.httpClient.Do(req)
	if err != nil { return nil, makeSimpleError(err.Error()) }
	defer resp.Body.Close()
	if resp.StatusCode == 429 {
		apiErr := &APIError{}
		apiErr.Error.Code, apiErr.Error.Message, apiErr.Error.Status = 429, "Quota exceeded", "RESOURCE_EXHAUSTED"
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
	if err := json.Unmarshal(body, &res); err != nil { return nil, makeSimpleError(fmt.Sprintf("JSON parse error: %v (Status: %d)", err, resp.StatusCode)) }
	if res.Error != nil {
		apiErr := &APIError{}
		apiErr.Error.Code, apiErr.Error.Message, apiErr.Error.Status = res.Error.Code, res.Error.Message, res.Error.Status
		return nil, apiErr
	}
	if resp.StatusCode != 200 { return nil, makeSimpleError(fmt.Sprintf("HTTP Error %d", resp.StatusCode)) }
	return res.Documents, nil
}

func (a *MirrorApp) normalizeForAPI(u string) string {
	name := strings.TrimPrefix(u, "https://")
	name = strings.TrimPrefix(name, "http://")
	if strings.HasPrefix(name, "cloud.google.com/") { name = "docs.cloud.google.com/" + strings.TrimPrefix(name, "cloud.google.com/") }
	return "documents/" + name
}

func (a *MirrorApp) handleLeafFailure(u string, wg *sync.WaitGroup) error {
	a.httpSem <- struct{}{}
	defer func() { <-a.httpSem }()

	curr := u
	lastStatus := 0
	for i := 0; i < 10; i++ {
		a.recordHTTPRequest()
		resp, err := a.noRedirectClient.Get(curr)
		if err != nil { break }
		lastStatus = resp.StatusCode
		if resp.StatusCode/100 != 3 {
			resp.Body.Close()
			break
		}
		loc := resp.Header.Get("Location")
		resp.Body.Close()
		if loc == "" { break }
		if strings.HasPrefix(loc, "/") { loc = "https://docs.cloud.google.com" + loc }
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
		a.failedURLs[u] = lastStatus
		atomic.AddInt32(&a.failedCount, 1)
		a.mu.Unlock()
		lpath := filepath.Join(a.cfg.DocsDir, strings.TrimPrefix(a.normalizeForAPI(u), "documents/") + ".md")
		os.Remove(lpath)
	}
	atomic.AddInt32(&a.inflightCount, -1)
	atomic.AddInt32(&a.finishedCount, 1)
	a.markActivity()
	wg.Done()
	return nil
}

func (a *MirrorApp) loadMasterListOnly() {
	processed, _ := a.storage.LoadProcessedURLs()
	a.mu.Lock()
	for k, v := range processed { a.processedURLs[k] = v }
	a.mu.Unlock()
}

func makeSimpleError(msg string) *APIError {
	e := &APIError{}
	e.Error.Message = msg
	return e
}
