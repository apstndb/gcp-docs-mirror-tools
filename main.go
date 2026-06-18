package main

import (
	"bufio"
	"context"
	"errors"
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
	"github.com/apstndb/developerknowledge-go"
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
	APIKey            string        `toml:"-"`
	DocsDir           string        `toml:"docs_dir"`
	LogDir            string        `toml:"log_dir"`
	MetadataFile      string        `toml:"metadata_file"`
	Recursive         bool          `toml:"recursive"`
	Refresh           bool          `toml:"refresh"`
	Resume            bool          `toml:"resume"`
	Discovery         bool          `toml:"discovery"`
	Verbose           bool          `toml:"verbose"`
	IncludeUpdateTime bool          `toml:"include_update_time"`
	Prefixes          []string      `toml:"prefixes"`
	Seeds             []string      `toml:"seeds"`
	Sitemaps          []string      `toml:"sitemaps"`
	QuotaPerMinute    float64       `toml:"qpm"`
	QuotaWait         time.Duration `toml:"qw"`
	SpannerDB         string        `toml:"spanner_db"`
	StallTimeout      time.Duration `toml:"stall_timeout"`
	// ExtraHosts appends to the default Developer Knowledge corpus hosts.
	ExtraHosts []string `toml:"extra_hosts"`
	// DefaultHost is the host assumed for relative links and path-only prefixes.
	DefaultHost string `toml:"default_host"`
}

// defaultKnownHosts returns the Developer Knowledge API corpus domains.
// Source: https://developers.google.com/knowledge/reference/corpus-reference
func defaultKnownHosts() []string {
	return []string{
		"adk.dev",
		"ai.google.dev",
		"antigravity.google",
		"developer.android.com",
		"developer.chrome.com",
		"developers.google.com",
		"developers.home.google.com",
		"docs.apigee.com",
		"docs.cloud.google.com",
		"firebase.google.com",
		"fuchsia.dev",
		"geminicli.com",
		"go.dev",
		"web.dev",
		"www.tensorflow.org",
	}
}

// defaultHostAliases maps legacy or alternate hostnames to their canonical
// Developer Knowledge corpus host.
func defaultHostAliases() map[string]string {
	return map[string]string{
		"cloud.google.com": "docs.cloud.google.com",
	}
}

func DefaultConfig() *Config {
	return &Config{
		DocsDir:        "docs",
		LogDir:         "logs",
		MetadataFile:   "metadata.yaml",
		Recursive:      false,
		Refresh:        false,
		Discovery:      true,
		Prefixes:       []string{"/spanner/docs/"},
		Sitemaps:       nil,
		QuotaPerMinute: 50.0,
		QuotaWait:      70 * time.Second,
		StallTimeout:   0,
		DefaultHost:    "docs.cloud.google.com",
	}
}

type Storage interface {
	Save(docs ...Document) error
	LoadProcessedURLs() (map[string]bool, error)
}

type ProcessedURLUpdateTimeLoader interface {
	LoadProcessedURLUpdateTimes() (map[string]string, error)
}

type DiskStorage struct {
	docsDir           string
	logDir            string
	includeUpdateTime bool
}

func (s *DiskStorage) Save(docs ...Document) error {
	for _, doc := range docs {
		relPath := strings.TrimPrefix(doc.Name, "documents/")
		fullPath := filepath.Join(s.docsDir, relPath+".md")
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			return err
		}
		content, err := formatDocumentForStorage(doc, s.includeUpdateTime)
		if err != nil {
			return err
		}
		if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
			return err
		}
	}
	return nil
}

func (s *DiskStorage) LoadProcessedURLs() (map[string]bool, error) {
	updateTimes, err := s.LoadProcessedURLUpdateTimes()
	if err != nil {
		return nil, err
	}
	processed := make(map[string]bool, len(updateTimes))
	for u := range updateTimes {
		processed[u] = true
	}
	return processed, nil
}

func (s *DiskStorage) LoadProcessedURLUpdateTimes() (map[string]string, error) {
	if s.logDir == "" {
		return make(map[string]string), nil
	}
	f, err := os.Open(filepath.Join(s.logDir, "urls.txt"))
	if err != nil {
		return make(map[string]string), nil
	}
	defer f.Close()
	processed := make(map[string]string)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if url, updateTime := parseProcessedURLLogLine(scanner.Text()); url != "" {
			processed[url] = updateTime
		}
	}
	return processed, nil
}

type MirrorApp struct {
	cfg           *Config
	storage       Storage
	processedURLs map[string]bool
	updateTimes   map[string]string
	redirects     map[string]string
	failedURLs    map[string]int // URL -> HTTP StatusCode
	mdParser      goldmark.Markdown

	// Host handling
	knownHosts  map[string]bool   // canonical hosts accepted by the corpus
	hostAliases map[string]string // legacy host -> canonical host
	defaultHost string            // host used when none is encoded in the input
	prefixRules []prefixRule      // parsed cfg.Prefixes

	// Shared HTTP clients
	apiHTTPClient    *http.Client
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

	apiWindow        [60]int32
	httpWindow       [60]int32
	lastWindowUpdate int64

	lastActivity int64
	startTime    time.Time
	isCI         bool
}

type Document = dkapi.Document
type APIError = dkapi.APIError

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
	flag.BoolVar(&cfg.IncludeUpdateTime, "include-update-time", cfg.IncludeUpdateTime, "Include update_time in YAML frontmatter")
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

	apiHTTPClient, apiKey, err := newDeveloperKnowledgeHTTPClient(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing Developer Knowledge API client: %v\n", err)
		os.Exit(1)
	}
	cfg.APIKey = apiKey

	var storage Storage
	if cfg.SpannerDB != "" {
		storage, err = NewSpannerStorage(context.Background(), cfg.SpannerDB)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing Spanner: %v\n", err)
			os.Exit(1)
		}
	} else {
		storage = &DiskStorage{
			docsDir:           cfg.DocsDir,
			logDir:            cfg.LogDir,
			includeUpdateTime: cfg.IncludeUpdateTime,
		}
	}

	knownHosts := make(map[string]bool)
	for _, h := range defaultKnownHosts() {
		knownHosts[h] = true
	}
	for _, h := range cfg.ExtraHosts {
		if h = strings.TrimSpace(h); h != "" {
			knownHosts[h] = true
		}
	}
	hostAliases := defaultHostAliases()
	defaultHost := cfg.DefaultHost
	if defaultHost == "" {
		defaultHost = "docs.cloud.google.com"
	}
	if !knownHosts[defaultHost] {
		if canon, ok := hostAliases[defaultHost]; ok {
			defaultHost = canon
		} else {
			knownHosts[defaultHost] = true
		}
	}

	app := &MirrorApp{
		cfg:           cfg,
		storage:       storage,
		processedURLs: make(map[string]bool),
		updateTimes:   make(map[string]string),
		redirects:     make(map[string]string),
		failedURLs:    make(map[string]int),
		mdParser:      goldmark.New(),
		knownHosts:    knownHosts,
		hostAliases:   hostAliases,
		defaultHost:   defaultHost,
		apiHTTPClient: apiHTTPClient,
		httpClient:    &http.Client{Timeout: 30 * time.Second},
		noRedirectClient: &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		limiter:          rate.NewLimiter(rate.Limit(cfg.QuotaPerMinute/60.0), int(cfg.QuotaPerMinute)),
		apiSem:           make(chan struct{}, 8),  // Increased concurrency
		httpSem:          make(chan struct{}, 20), // Limit concurrent HTTP checks
		queueChan:        make(chan string, 10000),
		sessionQueued:    make(map[string]bool),
		startTime:        time.Now(),
		lastActivity:     time.Now().UnixNano(),
		lastWindowUpdate: time.Now().Unix(),
		isCI:             os.Getenv("CI") == "true",
	}
	app.prefixRules = app.parsePrefixes(cfg.Prefixes)

	stopProgress := make(chan struct{})
	progressDone := make(chan struct{})
	go app.reportProgress(stopProgress, progressDone)

	if err := app.Run(context.Background(), seeds); err != nil {
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
		if last == now {
			break
		}
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
		if last == now {
			break
		}
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
	if !a.isCI {
		fmt.Print("\r\033[K")
	}
	fmt.Printf(format+"\n", args...)
	if !a.isCI {
		a.drawProgressLocked()
	}
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
	if disc > 0 {
		percent = float64(done) / float64(disc) * 100
	}

	elapsedSecs := time.Since(a.startTime).Seconds()
	overallRate := 0.0
	if elapsedSecs > 0 {
		overallRate = float64(done) / elapsedSecs
	}

	eta := "??:??"
	if overallRate > 0 && disc > done {
		remaining := float64(disc-done) / overallRate
		d := time.Duration(remaining) * time.Second
		eta = fmt.Sprintf("%02d:%02d", int(d.Minutes()), int(d.Seconds())%60)
	}

	status := ""
	if waiting {
		status = "[WAITING QUOTA] "
	}

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
	if a.isCI {
		interval = 10 * time.Second
	}
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

func (a *MirrorApp) Run(ctx context.Context, seeds []string) error {
	a.log("GCP Docs Mirror Tool %s (%s) built at %s", Version, Commit, BuildTime)
	a.log("Starting mirror process...")
	a.log("  - Seeds:    %d", len(seeds))
	a.log("  - Prefixes: %v", a.cfg.Prefixes)
	a.log("  - Quota:    %.1f QPM", a.cfg.QuotaPerMinute)
	if a.cfg.SpannerDB != "" {
		a.log("  - Storage:  Spanner (%s)", a.cfg.SpannerDB)
	} else {
		a.log("  - Storage:  Disk (%s)", a.cfg.DocsDir)
	}

	if a.cfg.Resume {
		a.loadMasterListOnly()
	}
	var activeWork sync.WaitGroup
	processDone := make(chan error, 1)
	go func() { processDone <- a.processStream(ctx, &activeWork) }()

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
			for _, s := range seeds {
				islands = append(islands, a.fetchAndExtractLinks(s, []string{"devsite-tabs-wrapper"})...)
			}
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
		for u := range a.processedURLs {
			existing = append(existing, u)
		}
		a.mu.Unlock()
		a.enqueueBatch(existing, &activeWork)
	}

	stopSaver := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				a.saveMetadata()
			case <-stopSaver:
				return
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
	for _, raw := range urls {
		u := a.resolveAndNormalize(raw, "")
		if u == "" || !a.matchesAnyPrefix(u) {
			continue
		}
		if a.sessionQueued[u] {
			continue
		}
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

func (a *MirrorApp) processStream(ctx context.Context, wg *sync.WaitGroup) error {
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
				if err := a.processBatchRecursive(ctx, b, wg); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
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
			if len(currentBatch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (a *MirrorApp) saveMetadata() {
	if a.cfg.LogDir != "" {
		os.MkdirAll(a.cfg.LogDir, 0755)
	}

	a.mu.Lock()
	var urls []string
	for k := range a.processedURLs {
		urls = append(urls, k)
	}
	sort.Strings(urls)
	urlLines := make([]string, 0, len(urls))
	for _, u := range urls {
		urlLines = append(urlLines, formatProcessedURLLogLine(u, a.updateTimes[u]))
	}

	var fails []string
	for k, v := range a.failedURLs {
		fails = append(fails, fmt.Sprintf("%d %s", v, k))
	}
	sort.Strings(fails)

	var rs []string
	for k, v := range a.redirects {
		rs = append(rs, k+" "+v)
	}
	sort.Strings(rs)
	a.mu.Unlock()

	if a.cfg.LogDir != "" {
		os.WriteFile(filepath.Join(a.cfg.LogDir, "urls.txt"), []byte(strings.Join(urlLines, "\n")+"\n"), 0644)
		os.WriteFile(filepath.Join(a.cfg.LogDir, "failed.txt"), []byte(strings.Join(fails, "\n")+"\n"), 0644)
		os.WriteFile(filepath.Join(a.cfg.LogDir, "redirects.txt"), []byte(strings.Join(rs, "\n")+"\n"), 0644)
	}

	fileCount := 0
	filepath.Walk(a.cfg.DocsDir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Ext(info.Name()) == ".md" {
			fileCount++
		}
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

func (a *MirrorApp) takeTokens(ctx context.Context, n int) error {
	doneWaiting := a.beginQuotaWait()
	defer doneWaiting()
	return a.limiter.WaitN(ctx, n)
}

func (a *MirrorApp) beginQuotaWait() func() {
	atomic.AddInt32(&a.isWaitingQuota, 1)
	a.markActivity()
	return func() {
		atomic.AddInt32(&a.isWaitingQuota, -1)
	}
}

func (a *MirrorApp) fetchAndExtractLinks(u string, targetClasses []string) []string {
	a.recordHTTPRequest()
	resp, err := a.httpClient.Get(u)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	baseURL := a.resolveAndNormalize(resp.Request.URL.String(), "")
	var results []string
	for _, l := range a.extractLinksWithClassFilter(resp.Body, targetClasses) {
		if normalized := a.resolveAndNormalize(l, baseURL); normalized != "" && a.matchesAnyPrefix(normalized) {
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
		case html.ErrorToken:
			return links
		case html.StartTagToken:
			depth++
			t := z.Token()
			if inTargetDepth == 0 {
				for _, attr := range t.Attr {
					if attr.Key == "class" {
						for _, c := range strings.Fields(attr.Val) {
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
		relToDocs = filepath.ToSlash(strings.TrimSuffix(relToDocs, ".md"))
		host, rest, _ := strings.Cut(relToDocs, "/")
		canonicalHost := a.canonicalHost(host)
		if canonicalHost == "" {
			// Legacy layout without a host directory: treat as default host.
			canonicalHost = a.defaultHost
			rest = relToDocs
		}
		baseURL := a.makeCanonical(canonicalHost, "/"+rest)
		content, _ := os.ReadFile(fpath)
		for _, l := range a.extractLinksFromMarkdown(content) {
			if normalized := a.resolveAndNormalize(l, baseURL); normalized != "" && !a.isProcessedSession(normalized) && a.matchesAnyPrefix(normalized) {
				allDiscovered = append(allDiscovered, normalized)
			}
		}
		return nil
	})
	return allDiscovered
}

// prefixRule represents a parsed prefix used for matching URLs.
// When host is empty the rule matches any known host.
type prefixRule struct {
	host string
	path string // always begins with "/"
}

func (a *MirrorApp) parsePrefixes(prefixes []string) []prefixRule {
	rules := make([]prefixRule, 0, len(prefixes))
	for _, p := range prefixes {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		rules = append(rules, a.parsePrefix(p))
	}
	return rules
}

func (a *MirrorApp) parsePrefix(p string) prefixRule {
	if strings.HasPrefix(p, "/") {
		return prefixRule{host: "", path: a.cleanPath(p)}
	}
	if h, pa := a.parseCanonical(p); h != "" {
		return prefixRule{host: h, path: pa}
	}
	// Fallback: treat as a host-less path.
	return prefixRule{host: "", path: a.cleanPath("/" + p)}
}

func (a *MirrorApp) canonicalHost(h string) string {
	h = strings.ToLower(strings.TrimSpace(h))
	if h == "" {
		return ""
	}
	if alias, ok := a.hostAliases[h]; ok {
		h = alias
	}
	if a.knownHosts[h] {
		return h
	}
	return ""
}

// cleanPath returns a normalized URL path beginning with "/".
// It strips the .md suffix and any trailing slash on non-root paths.
func (a *MirrorApp) cleanPath(p string) string {
	p = strings.Split(p, "#")[0]
	p = strings.Split(p, "?")[0]
	p = strings.TrimSuffix(p, ".md")
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	p = path.Clean(p)
	if p == "." {
		p = "/"
	}
	return p
}

// parseCanonical parses a URL-like string and returns its canonical host
// and cleaned path. Returns ("", "") when the host is not recognized.
// Accepts inputs with or without an explicit scheme.
func (a *MirrorApp) parseCanonical(u string) (host, urlPath string) {
	u = strings.TrimSpace(u)
	if u == "" {
		return "", ""
	}
	// Protocol-relative
	if strings.HasPrefix(u, "//") {
		u = "https:" + u
	}
	// Bare host[/path] without scheme: detect and synthesize https://
	if !strings.Contains(u, "://") {
		head := u
		if i := strings.Index(head, "/"); i >= 0 {
			head = head[:i]
		}
		if a.canonicalHost(head) != "" {
			u = "https://" + u
		} else {
			return "", ""
		}
	}
	pu, err := url.Parse(u)
	if err != nil || pu.Host == "" {
		return "", ""
	}
	h := a.canonicalHost(pu.Hostname())
	if h == "" {
		return "", ""
	}
	return h, a.cleanPath(pu.Path)
}

// makeCanonical assembles a canonical URL from host and path.
func (a *MirrorApp) makeCanonical(host, urlPath string) string {
	if host == "" {
		return ""
	}
	return "https://" + host + a.cleanPath(urlPath)
}

// urlPath returns the cleaned path component of u, or cleans u when no host
// is present. Useful for prefix matching against a URL or a bare path.
func (a *MirrorApp) urlPath(u string) string {
	if h, p := a.parseCanonical(u); h != "" {
		return p
	}
	return a.cleanPath(u)
}

// urlHost returns the canonical host of u, or "" if unknown.
func (a *MirrorApp) urlHost(u string) string {
	h, _ := a.parseCanonical(u)
	return h
}

// resolveAndNormalize resolves link against baseURL (a canonical URL or
// empty) and returns a canonical URL, or "" if the host is not recognized
// or the link cannot be resolved.
func (a *MirrorApp) resolveAndNormalize(link, baseURL string) string {
	baseHost, basePath := a.parseCanonical(baseURL)
	if baseHost == "" {
		baseHost = a.defaultHost
		basePath = "/"
	}
	link = strings.Split(link, "#")[0]
	if link == "" {
		return a.makeCanonical(baseHost, basePath)
	}
	// Absolute URL (including protocol-relative).
	if strings.Contains(link, "://") || strings.HasPrefix(link, "//") {
		h, p := a.parseCanonical(link)
		if h == "" {
			return ""
		}
		return a.makeCanonical(h, p)
	}
	// Root-relative
	if strings.HasPrefix(link, "/") {
		return a.makeCanonical(baseHost, link)
	}
	// Document-relative: existing convention treats basePath as a directory.
	return a.makeCanonical(baseHost, path.Join(basePath, link))
}

func (a *MirrorApp) matchesAnyPrefix(u string) bool {
	uHost, uPath := a.parseCanonical(u)
	if uHost == "" {
		return false
	}
	rules := a.prefixRules
	if len(rules) == 0 {
		rules = a.parsePrefixes(a.cfg.Prefixes)
	}
	for _, r := range rules {
		if r.host != "" && r.host != uHost {
			continue
		}
		cleanP := strings.TrimSuffix(r.path, "/")
		if cleanP == "" {
			return true
		}
		if uPath == cleanP || strings.HasPrefix(uPath, cleanP+"/") {
			return true
		}
	}
	return false
}

func (a *MirrorApp) extractLinksFromMarkdown(source []byte) []string {
	var links []string
	source = stripLeadingFrontmatter(source)
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

func (a *MirrorApp) processBatchRecursive(ctx context.Context, urls []string, wg *sync.WaitGroup) error {
	if len(urls) == 0 {
		return nil
	}
	docs, err := a.fetchDocsWithRetry(ctx, urls)
	if err == nil {
		if err := a.storage.Save(docs...); err != nil {
			a.log("Storage error: %v", err)
		}
		a.mu.Lock()
		processedMap := make(map[string]bool)
		for _, doc := range docs {
			u := a.apiNameToURL(doc.Name)
			if u == "" {
				continue
			}
			a.processedURLs[u] = true
			a.updateTimes[u] = doc.UpdateTime
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
	if !dkapi.IsBisectableDocumentError(err) {
		a.finishBatchAPIError(urls, wg, err)
		return err
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
	errs := make(chan error, 2)
	go func() {
		errs <- a.processBatchRecursive(ctx, urls[:mid], wg)
	}()
	go func() {
		errs <- a.processBatchRecursive(ctx, urls[mid:], wg)
	}()
	leftErr, rightErr := <-errs, <-errs
	if leftErr == nil {
		return rightErr
	}
	if rightErr == nil {
		return leftErr
	}
	return errors.Join(leftErr, rightErr)
}

func (a *MirrorApp) finishBatchAPIError(urls []string, wg *sync.WaitGroup, err error) {
	a.log("Developer Knowledge API batch failed for %d URL(s): %v", len(urls), err)
	interrupted := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, u := range urls {
		if !interrupted {
			a.failedURLs[u] = -1
			atomic.AddInt32(&a.failedCount, 1)
		}
		atomic.AddInt32(&a.inflightCount, -1)
		atomic.AddInt32(&a.finishedCount, 1)
		wg.Done()
	}
	a.markActivity()
}

func (a *MirrorApp) fetchDocsWithRetry(ctx context.Context, urls []string) ([]Document, error) {
	for i := 0; i < 5; i++ {
		docs, err := a.fetchDocs(ctx, urls)
		if err == nil {
			return docs, nil
		}

		var rateLimitErr *dkapi.RateLimitError
		var apiErr *APIError
		if errors.As(err, &rateLimitErr) || (errors.As(err, &apiErr) && (apiErr.Code == 429 || apiErr.Status == "RESOURCE_EXHAUSTED")) {
			a.log("Quota exceeded (429). Waiting %v for window reset (attempt %d/5)...", a.cfg.QuotaWait, i+1)
			doneWaiting := a.beginQuotaWait()
			err := dkapi.SleepContext(ctx, a.cfg.QuotaWait)
			doneWaiting()
			if err != nil {
				return nil, err
			}
			continue
		}
		return nil, err
	}
	return nil, makeSimpleError("Quota exceeded consistently after retries")
}

func (a *MirrorApp) fetchDocs(ctx context.Context, urls []string) ([]Document, error) {
	select {
	case a.apiSem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-a.apiSem }()
	if err := a.takeTokens(ctx, 1); err != nil {
		return nil, err
	}
	a.recordAPIRequest()
	names := make([]string, 0, len(urls))
	for _, u := range urls {
		names = append(names, a.normalizeForAPI(u))
	}

	client := &dkapi.Client{
		BaseURL:    dkapi.DefaultV1BaseURL,
		APIKey:     a.cfg.APIKey,
		HTTPClient: a.apiHTTPClient,
		Context:    ctx,
		MaxRetries: 0,
	}
	return client.BatchGetDocuments(names)
}

func (a *MirrorApp) normalizeForAPI(u string) string {
	h, p := a.parseCanonical(u)
	if h == "" {
		// Fallback: treat the input as a path under the default host.
		return "documents/" + a.defaultHost + a.cleanPath(u)
	}
	return "documents/" + h + p
}

// apiNameToURL reverses normalizeForAPI: "documents/HOST/PATH" -> canonical URL.
func (a *MirrorApp) apiNameToURL(name string) string {
	rest := strings.TrimPrefix(name, "documents/")
	host, pathPart, _ := strings.Cut(rest, "/")
	h := a.canonicalHost(host)
	if h == "" {
		return ""
	}
	return a.makeCanonical(h, "/"+pathPart)
}

func (a *MirrorApp) handleLeafFailure(u string, wg *sync.WaitGroup) error {
	a.httpSem <- struct{}{}
	defer func() { <-a.httpSem }()

	curr := u
	lastStatus := 0
	for i := 0; i < 10; i++ {
		a.recordHTTPRequest()
		resp, err := a.noRedirectClient.Get(curr)
		if err != nil {
			break
		}
		lastStatus = resp.StatusCode
		if resp.StatusCode/100 != 3 {
			resp.Body.Close()
			break
		}
		loc := resp.Header.Get("Location")
		resp.Body.Close()
		if loc == "" {
			break
		}
		if strings.HasPrefix(loc, "/") {
			h := a.urlHost(curr)
			if h == "" {
				h = a.defaultHost
			}
			loc = "https://" + h + loc
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
		a.failedURLs[u] = lastStatus
		atomic.AddInt32(&a.failedCount, 1)
		a.mu.Unlock()
		lpath := filepath.Join(a.cfg.DocsDir, strings.TrimPrefix(a.normalizeForAPI(u), "documents/")+".md")
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
	if loader, ok := a.storage.(ProcessedURLUpdateTimeLoader); ok {
		updateTimes, _ := loader.LoadProcessedURLUpdateTimes()
		a.mu.Lock()
		for k, v := range updateTimes {
			a.updateTimes[k] = v
		}
		a.mu.Unlock()
	}
	a.mu.Lock()
	for k, v := range processed {
		a.processedURLs[k] = v
	}
	a.mu.Unlock()
}

func makeSimpleError(msg string) *APIError {
	return &APIError{Message: msg}
}

func parseProcessedURLLogLine(line string) (string, string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", ""
	}
	parts := strings.SplitN(line, "\t", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], strings.TrimSpace(parts[1])
}

func formatProcessedURLLogLine(url, updateTime string) string {
	return url + "\t" + updateTime
}
