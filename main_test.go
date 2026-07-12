package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yuin/goldmark"
	"golang.org/x/time/rate"
)

func newTestApp(prefixes ...string) *MirrorApp {
	knownHosts := make(map[string]bool)
	for _, h := range defaultKnownHosts() {
		knownHosts[h] = true
	}
	app := &MirrorApp{
		cfg:         &Config{Prefixes: prefixes},
		knownHosts:  knownHosts,
		hostAliases: defaultHostAliases(),
		defaultHost: "docs.cloud.google.com",
	}
	app.prefixRules = app.parsePrefixes(prefixes)
	return app
}

type mirrorRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f mirrorRoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type stubStorage struct {
	err error
}

func (s stubStorage) Save(...Document) error {
	return s.err
}

func (stubStorage) LoadProcessedURLs() (map[string]bool, error) {
	return nil, nil
}

func newFetchTestApp(rt http.RoundTripper) *MirrorApp {
	app := newTestApp()
	app.cfg.QuotaWait = time.Hour
	app.apiHTTPClient = &http.Client{Transport: rt}
	app.apiSem = make(chan struct{}, 1)
	app.limiter = rate.NewLimiter(rate.Inf, 1)
	app.failedURLs = make(map[string]int)
	app.processedURLs = make(map[string]bool)
	app.updateTimes = make(map[string]string)
	app.redirects = make(map[string]string)
	app.startTime = time.Now()
	app.lastActivity = time.Now().UnixNano()
	app.isCI = true
	return app
}

func TestProcessBatchRecursiveDoesNotBisectNonDocumentAPIError(t *testing.T) {
	var requests int32
	app := newFetchTestApp(mirrorRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&requests, 1)
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body: io.NopCloser(strings.NewReader(`{
				"error": {
					"code": 500,
					"message": "backend unavailable",
					"status": "INTERNAL"
				}
			}`)),
		}, nil
	}))

	urls := []string{
		"https://docs.cloud.google.com/spanner/docs/a",
		"https://docs.cloud.google.com/spanner/docs/b",
	}
	var wg sync.WaitGroup
	wg.Add(len(urls))
	atomic.StoreInt32(&app.inflightCount, int32(len(urls)))

	err := app.processBatchRecursive(context.Background(), urls, &wg)
	if err == nil {
		t.Fatal("expected non-bisectable API error")
	}
	wg.Wait()

	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&app.failedCount); got != int32(len(urls)) {
		t.Fatalf("failedCount = %d, want %d", got, len(urls))
	}
	if got := atomic.LoadInt32(&app.finishedCount); got != int32(len(urls)) {
		t.Fatalf("finishedCount = %d, want %d", got, len(urls))
	}
	if got := atomic.LoadInt32(&app.inflightCount); got != 0 {
		t.Fatalf("inflightCount = %d, want 0", got)
	}
	for _, u := range urls {
		if got := app.failedURLs[u]; got != failedStatusAPI {
			t.Fatalf("failedURLs[%q] = %d, want %d", u, got, failedStatusAPI)
		}
	}
}

func TestProcessBatchRecursiveFinishesWorkAfterStorageError(t *testing.T) {
	storageErr := errors.New("disk full")
	app := newFetchTestApp(mirrorRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{
				"documents": [{
					"name": "documents/docs.cloud.google.com/spanner/docs/a",
					"content": "# A"
				}]
			}`)),
		}, nil
	}))
	app.storage = stubStorage{err: storageErr}
	urls := []string{"https://docs.cloud.google.com/spanner/docs/a"}
	var wg sync.WaitGroup
	wg.Add(len(urls))
	atomic.StoreInt32(&app.inflightCount, int32(len(urls)))

	err := app.processBatchRecursive(context.Background(), urls, &wg)
	if !errors.Is(err, storageErr) {
		t.Fatalf("error = %v, want %v", err, storageErr)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("work remained active after storage error")
	}

	if got := atomic.LoadInt32(&app.failedCount); got != int32(len(urls)) {
		t.Errorf("failedCount = %d, want %d", got, len(urls))
	}
	if got := atomic.LoadInt32(&app.finishedCount); got != int32(len(urls)) {
		t.Errorf("finishedCount = %d, want %d", got, len(urls))
	}
	if got := atomic.LoadInt32(&app.inflightCount); got != 0 {
		t.Errorf("inflightCount = %d, want 0", got)
	}
	if app.processedURLs[urls[0]] {
		t.Errorf("processedURLs[%q] = true after storage error", urls[0])
	}
	if _, ok := app.failedURLs[urls[0]]; ok {
		t.Errorf("failedURLs[%q] persisted a retryable storage error", urls[0])
	}
}

func TestProcessBatchRecursiveFollowsLegacyCloudDocsRedirect(t *testing.T) {
	const (
		legacyURL    = "https://cloud.google.com/spanner/docs"
		canonicalURL = "https://docs.cloud.google.com/spanner/docs"
	)
	app := newFetchTestApp(mirrorRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		names := req.URL.Query()["names"]
		if len(names) != 1 {
			t.Fatalf("names = %v, want one document name", names)
		}
		switch names[0] {
		case "documents/cloud.google.com/spanner/docs":
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body: io.NopCloser(strings.NewReader(`{
					"error": {
						"code": 404,
						"message": "requested entity was not found",
						"status": "NOT_FOUND"
					}
				}`)),
			}, nil
		case "documents/docs.cloud.google.com/spanner/docs":
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(`{
					"documents": [{
						"name": "documents/docs.cloud.google.com/spanner/docs",
						"content": "# Spanner documentation"
					}]
				}`)),
			}, nil
		default:
			t.Fatalf("unexpected document name %q", names[0])
			return nil, nil
		}
	}))
	app.storage = stubStorage{}
	app.cfg.DocsDir = t.TempDir()
	app.cfg.Prefixes = []string{"/spanner/docs/"}
	app.prefixRules = app.parsePrefixes(app.cfg.Prefixes)
	app.httpSem = make(chan struct{}, 1)
	app.queueChan = make(chan string, 1)
	app.sessionQueued = make(map[string]bool)
	app.noRedirectClient = &http.Client{
		Transport: mirrorRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.String() {
			case legacyURL:
				return &http.Response{
					StatusCode: http.StatusMovedPermanently,
					Header:     http.Header{"Location": []string{canonicalURL}},
					Body:       io.NopCloser(strings.NewReader("")),
				}, nil
			case canonicalURL:
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("")),
				}, nil
			default:
				t.Errorf("unexpected HTTP probe URL %q", req.URL.String())
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader("")),
				}, nil
			}
		}),
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	atomic.StoreInt32(&app.inflightCount, 1)
	if err := app.processBatchRecursive(context.Background(), []string{legacyURL}, &wg); err != nil {
		t.Fatalf("legacy process error: %v", err)
	}
	app.redirectWG.Wait()

	var redirectedURL string
	select {
	case redirectedURL = <-app.queueChan:
	case <-time.After(time.Second):
		t.Fatal("redirect destination was not enqueued")
	}
	if redirectedURL != canonicalURL {
		t.Fatalf("redirected URL = %q, want %q", redirectedURL, canonicalURL)
	}
	if err := app.processBatchRecursive(context.Background(), []string{redirectedURL}, &wg); err != nil {
		t.Fatalf("redirect destination process error: %v", err)
	}
	wg.Wait()

	if got := app.redirects[legacyURL]; got != canonicalURL {
		t.Errorf("redirects[%q] = %q, want %q", legacyURL, got, canonicalURL)
	}
	if !app.processedURLs[canonicalURL] {
		t.Errorf("processedURLs[%q] = false", canonicalURL)
	}
	if _, ok := app.failedURLs[legacyURL]; ok {
		t.Errorf("failedURLs[%q] was persisted after successful redirect", legacyURL)
	}
	if got := atomic.LoadInt32(&app.redirectCount); got != 1 {
		t.Errorf("redirectCount = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&app.failedCount); got != 0 {
		t.Errorf("failedCount = %d, want 0", got)
	}
	if got := atomic.LoadInt32(&app.finishedCount); got != 2 {
		t.Errorf("finishedCount = %d, want 2", got)
	}
	if got := atomic.LoadInt32(&app.inflightCount); got != 0 {
		t.Errorf("inflightCount = %d, want 0", got)
	}
}

func TestFinishBatchStorageErrorDoesNotCountContextInterruption(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "canceled", err: context.Canceled},
		{name: "deadline exceeded", err: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := newFetchTestApp(nil)
			urls := []string{
				"https://docs.cloud.google.com/spanner/docs/a",
				"https://docs.cloud.google.com/spanner/docs/b",
			}
			var wg sync.WaitGroup
			wg.Add(len(urls))
			atomic.StoreInt32(&app.inflightCount, int32(len(urls)))

			app.finishBatchStorageError(urls, &wg, tc.err)
			wg.Wait()

			if got := atomic.LoadInt32(&app.failedCount); got != 0 {
				t.Errorf("failedCount = %d, want 0", got)
			}
			if got := atomic.LoadInt32(&app.finishedCount); got != int32(len(urls)) {
				t.Errorf("finishedCount = %d, want %d", got, len(urls))
			}
			if got := atomic.LoadInt32(&app.inflightCount); got != 0 {
				t.Errorf("inflightCount = %d, want 0", got)
			}
		})
	}
}

func TestRequestWindowPreservesRecentHistoryAcrossGap(t *testing.T) {
	app := &MirrorApp{lastWindowUpdate: 100}
	app.apiWindow[99%60] = 7
	app.apiWindow[100%60] = 3
	app.apiWindow[101%60] = 11
	app.apiWindow[102%60] = 13

	app.recordWindowRequest(102, &app.apiWindow)

	if got := app.apiWindow[99%60]; got != 7 {
		t.Errorf("apiWindow[99] = %d, want 7", got)
	}
	if got := app.apiWindow[100%60]; got != 3 {
		t.Errorf("apiWindow[100] = %d, want 3", got)
	}
	if got := app.apiWindow[101%60]; got != 0 {
		t.Errorf("apiWindow[101] = %d, want 0", got)
	}
	if got := app.apiWindow[102%60]; got != 1 {
		t.Errorf("apiWindow[102] = %d, want 1", got)
	}
}

func TestRequestWindowCountsConcurrentRequests(t *testing.T) {
	const requests = 1000
	app := &MirrorApp{lastWindowUpdate: 100}
	var wg sync.WaitGroup
	for range requests {
		wg.Go(func() {
			app.recordWindowRequest(101, &app.apiWindow)
		})
	}
	wg.Wait()

	if got := app.apiWindow[101%60]; got != requests {
		t.Errorf("apiWindow[101] = %d, want %d", got, requests)
	}
}

func TestFinishBatchAPIErrorDoesNotPersistContextInterruption(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "canceled", err: context.Canceled},
		{name: "deadline exceeded", err: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := newFetchTestApp(nil)
			urls := []string{
				"https://docs.cloud.google.com/spanner/docs/a",
				"https://docs.cloud.google.com/spanner/docs/b",
			}
			var wg sync.WaitGroup
			wg.Add(len(urls))
			atomic.StoreInt32(&app.inflightCount, int32(len(urls)))

			app.finishBatchAPIError(urls, &wg, tc.err)
			wg.Wait()

			if got := atomic.LoadInt32(&app.failedCount); got != 0 {
				t.Fatalf("failedCount = %d, want 0", got)
			}
			if got := atomic.LoadInt32(&app.finishedCount); got != int32(len(urls)) {
				t.Fatalf("finishedCount = %d, want %d", got, len(urls))
			}
			if got := atomic.LoadInt32(&app.inflightCount); got != 0 {
				t.Fatalf("inflightCount = %d, want 0", got)
			}
			for _, u := range urls {
				if _, ok := app.failedURLs[u]; ok {
					t.Fatalf("failedURLs[%q] was persisted for context interruption", u)
				}
			}
		})
	}
}

func TestProcessBatchRecursivePropagatesSplitChildAPIError(t *testing.T) {
	var requests int32
	app := newFetchTestApp(mirrorRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&requests, 1)
		statusCode := http.StatusInternalServerError
		status := "INTERNAL"
		code := "500"
		if len(req.URL.Query()["names"]) == 2 {
			statusCode = http.StatusBadRequest
			status = "INVALID_ARGUMENT"
			code = "400"
		}
		return &http.Response{
			StatusCode: statusCode,
			Body: io.NopCloser(strings.NewReader(`{
				"error": {
					"code": ` + code + `,
					"message": "backend unavailable",
					"status": "` + status + `"
				}
			}`)),
		}, nil
	}))

	urls := []string{
		"https://docs.cloud.google.com/spanner/docs/a",
		"https://docs.cloud.google.com/spanner/docs/b",
	}
	var wg sync.WaitGroup
	wg.Add(len(urls))
	atomic.StoreInt32(&app.inflightCount, int32(len(urls)))

	err := app.processBatchRecursive(context.Background(), urls, &wg)
	if err == nil {
		t.Fatal("expected split child API error")
	}
	wg.Wait()

	if got := atomic.LoadInt32(&requests); got != 3 {
		t.Fatalf("requests = %d, want 3", got)
	}
	if got := atomic.LoadInt32(&app.failedCount); got != int32(len(urls)) {
		t.Fatalf("failedCount = %d, want %d", got, len(urls))
	}
	if got := atomic.LoadInt32(&app.finishedCount); got != int32(len(urls)) {
		t.Fatalf("finishedCount = %d, want %d", got, len(urls))
	}
	for _, u := range urls {
		if got := app.failedURLs[u]; got != failedStatusAPI {
			t.Fatalf("failedURLs[%q] = %d, want %d", u, got, failedStatusAPI)
		}
	}
}

func TestFetchDocsWithRetryCancelsQuotaWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var requests int32
	app := newFetchTestApp(mirrorRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&requests, 1)
		cancel()
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header:     http.Header{"Retry-After": []string{"3600"}},
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	}))
	_, err := app.fetchDocsWithRetry(ctx, []string{"https://docs.cloud.google.com/spanner/docs"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&app.isWaitingQuota); got != 0 {
		t.Fatalf("isWaitingQuota = %d, want 0", got)
	}
}

func TestFetchDocsHonorsContextWhileWaitingForAPISem(t *testing.T) {
	var requests int32
	app := newFetchTestApp(mirrorRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&requests, 1)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	}))
	app.apiSem <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := app.fetchDocs(ctx, []string{"https://docs.cloud.google.com/spanner/docs"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if got := atomic.LoadInt32(&requests); got != 0 {
		t.Fatalf("requests = %d, want 0", got)
	}
	if got := atomic.LoadInt32(&app.apiReqCount); got != 0 {
		t.Fatalf("apiReqCount = %d, want 0", got)
	}
}

func TestFetchDocsHonorsContextWhileWaitingForRateLimiter(t *testing.T) {
	var requests int32
	app := newFetchTestApp(mirrorRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&requests, 1)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	}))
	app.limiter = rate.NewLimiter(rate.Every(time.Hour), 1)
	if !app.limiter.Allow() {
		t.Fatal("failed to consume initial limiter token")
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		_, err := app.fetchDocs(ctx, []string{"https://docs.cloud.google.com/spanner/docs"})
		errCh <- err
	}()

	deadline := time.After(time.Second)
	for atomic.LoadInt32(&app.isWaitingQuota) == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for rate limiter wait")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fetchDocs did not return after context cancellation")
	}
	if got := atomic.LoadInt32(&requests); got != 0 {
		t.Fatalf("requests = %d, want 0", got)
	}
	if got := atomic.LoadInt32(&app.apiReqCount); got != 0 {
		t.Fatalf("apiReqCount = %d, want 0", got)
	}
	if got := atomic.LoadInt32(&app.isWaitingQuota); got != 0 {
		t.Fatalf("isWaitingQuota = %d, want 0", got)
	}
}

func TestTakeTokensTracksConcurrentQuotaWaiters(t *testing.T) {
	app := newFetchTestApp(nil)
	app.limiter = rate.NewLimiter(rate.Every(time.Hour), 1)
	if !app.limiter.Allow() {
		t.Fatal("failed to consume initial limiter token")
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	errCh := make(chan error, 2)

	go func() { errCh <- app.takeTokens(ctx1, 1) }()
	go func() { errCh <- app.takeTokens(ctx2, 1) }()
	waitForQuotaWaiters(t, app, 2)

	cancel1()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("first error = %v, want context.Canceled", err)
	}
	waitForQuotaWaiters(t, app, 1)

	cancel2()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("second error = %v, want context.Canceled", err)
	}
	waitForQuotaWaiters(t, app, 0)
}

func waitForQuotaWaiters(t *testing.T, app *MirrorApp, want int32) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		if got := atomic.LoadInt32(&app.isWaitingQuota); got == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("isWaitingQuota = %d, want %d", atomic.LoadInt32(&app.isWaitingQuota), want)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestURLPath(t *testing.T) {
	app := newTestApp()
	tests := []struct {
		input    string
		expected string
	}{
		{"https://docs.cloud.google.com/spanner/docs", "/spanner/docs"},
		{"https://cloud.google.com/spanner/docs/", "/spanner/docs"},
		{"https://docs.cloud.google.com/spanner/docs/dml-versus-mutations.md", "/spanner/docs/dml-versus-mutations"},
		{"http://docs.cloud.google.com/spanner/docs#anchor", "/spanner/docs"},
		{"https://developers.google.com/gemini-code-assist/docs/overview", "/gemini-code-assist/docs/overview"},
		{"/spanner/docs/backup/", "/spanner/docs/backup"},
		{"spanner/docs", "/spanner/docs"},
	}

	for _, tt := range tests {
		result := app.urlPath(tt.input)
		if result != tt.expected {
			t.Errorf("urlPath(%q) = %q; want %q", tt.input, result, tt.expected)
		}
	}
}

func TestCanonicalHost(t *testing.T) {
	app := newTestApp()
	tests := []struct {
		input    string
		expected string
	}{
		{"docs.cloud.google.com", "docs.cloud.google.com"},
		{"cloud.google.com", "cloud.google.com"},
		{"developers.google.com", "developers.google.com"},
		{"DOCS.CLOUD.GOOGLE.COM", "docs.cloud.google.com"},
		{"example.com", ""},
		{"", ""},
	}
	for _, tt := range tests {
		got := app.canonicalHost(tt.input)
		if got != tt.expected {
			t.Errorf("canonicalHost(%q) = %q; want %q", tt.input, got, tt.expected)
		}
	}
}

func TestResolveAndNormalize(t *testing.T) {
	app := newTestApp()
	tests := []struct {
		link     string
		base     string
		expected string
	}{
		{"concepts", "https://docs.cloud.google.com/spanner/docs", "https://docs.cloud.google.com/spanner/docs/concepts"},
		{"backup/", "https://docs.cloud.google.com/spanner/docs", "https://docs.cloud.google.com/spanner/docs/backup"},
		{"dml-versus-mutations.md", "https://docs.cloud.google.com/spanner/docs", "https://docs.cloud.google.com/spanner/docs/dml-versus-mutations"},
		{"/spanner/docs/concepts/", "https://docs.cloud.google.com/irrelevant", "https://docs.cloud.google.com/spanner/docs/concepts"},
		{"https://cloud.google.com/spanner/docs/", "https://docs.cloud.google.com/any", "https://cloud.google.com/spanner/docs"},
		{"#anchor", "https://docs.cloud.google.com/spanner/docs", "https://docs.cloud.google.com/spanner/docs"},
		// developers.google.com support
		{"set-up-gemini", "https://developers.google.com/gemini-code-assist/docs/overview", "https://developers.google.com/gemini-code-assist/docs/overview/set-up-gemini"},
		{"/gemini-code-assist/docs/quotas", "https://developers.google.com/anything", "https://developers.google.com/gemini-code-assist/docs/quotas"},
		{"https://developers.google.com/gemini-code-assist/docs/overview", "", "https://developers.google.com/gemini-code-assist/docs/overview"},
		// Cross-host absolute link preserves its host.
		{"https://docs.cloud.google.com/gemini/docs", "https://developers.google.com/gemini-code-assist/docs/overview", "https://docs.cloud.google.com/gemini/docs"},
		// Unknown host rejected.
		{"https://example.com/foo", "https://docs.cloud.google.com/spanner/docs", ""},
		// Empty base falls back to default host for relative paths.
		{"/spanner/docs", "", "https://docs.cloud.google.com/spanner/docs"},
	}

	for _, tt := range tests {
		result := app.resolveAndNormalize(tt.link, tt.base)
		if result != tt.expected {
			t.Errorf("resolveAndNormalize(%q, %q) = %q; want %q", tt.link, tt.base, result, tt.expected)
		}
	}
}

func TestNormalizeForAPI(t *testing.T) {
	app := newTestApp()
	tests := []struct {
		input    string
		expected string
	}{
		{"https://docs.cloud.google.com/spanner/docs", "documents/docs.cloud.google.com/spanner/docs"},
		{"https://cloud.google.com/spanner/docs", "documents/cloud.google.com/spanner/docs"},
		{"cloud.google.com/spanner/docs", "documents/cloud.google.com/spanner/docs"},
		{"https://developers.google.com/gemini-code-assist/docs/overview", "documents/developers.google.com/gemini-code-assist/docs/overview"},
		{"developers.google.com/gemini-code-assist/docs", "documents/developers.google.com/gemini-code-assist/docs"},
	}

	for _, tt := range tests {
		result := app.normalizeForAPI(tt.input)
		if result != tt.expected {
			t.Errorf("normalizeForAPI(%q) = %q; want %q", tt.input, result, tt.expected)
		}
	}
}

func TestAPINameToURL(t *testing.T) {
	app := newTestApp()
	tests := []struct {
		input    string
		expected string
	}{
		{"documents/docs.cloud.google.com/spanner/docs", "https://docs.cloud.google.com/spanner/docs"},
		{"documents/developers.google.com/gemini-code-assist/docs/overview", "https://developers.google.com/gemini-code-assist/docs/overview"},
		{"documents/example.com/foo", ""},
	}
	for _, tt := range tests {
		got := app.apiNameToURL(tt.input)
		if got != tt.expected {
			t.Errorf("apiNameToURL(%q) = %q; want %q", tt.input, got, tt.expected)
		}
	}
}

func TestMatchesAnyPrefix(t *testing.T) {
	app := newTestApp("/spanner/docs", "/sdk/gcloud/reference/spanner", "developers.google.com/gemini-code-assist/")
	tests := []struct {
		input    string
		expected bool
	}{
		{"https://docs.cloud.google.com/spanner/docs/overview", true},
		{"https://docs.cloud.google.com/spanner/docs", true},
		{"https://docs.cloud.google.com/sdk/gcloud/reference/spanner", true},
		{"https://docs.cloud.google.com/sdk/gcloud/reference/spanner/describe", true},
		{"https://docs.cloud.google.com/bigtable/docs", false},
		// Host-scoped prefix.
		{"https://developers.google.com/gemini-code-assist/docs/overview", true},
		{"https://developers.google.com/other-product/docs", false},
		// Host-scoped prefix doesn't match the wrong host.
		{"https://docs.cloud.google.com/gemini-code-assist/docs", false},
		// Unknown host rejected.
		{"https://example.com/spanner/docs", false},
	}

	for _, tt := range tests {
		result := app.matchesAnyPrefix(tt.input)
		if result != tt.expected {
			t.Errorf("matchesAnyPrefix(%q) = %v; want %v", tt.input, result, tt.expected)
		}
	}
}

func TestExtractLinksFromMarkdown(t *testing.T) {
	app := &MirrorApp{
		mdParser: goldmark.New(),
	}
	source := []byte(`
# Test
[Link 1](https://docs.cloud.google.com/spanner/docs)
[Link 2](/spanner/concepts)
<https://cloud.google.com/spanner/samples>
`)
	expected := []string{
		"https://docs.cloud.google.com/spanner/docs",
		"/spanner/concepts",
		"https://cloud.google.com/spanner/samples",
	}

	result := app.extractLinksFromMarkdown(source)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("extractLinksFromMarkdown() = %v; want %v", result, expected)
	}
}

func TestExtractLinksWithClassFilter(t *testing.T) {
	app := &MirrorApp{}
	htmlContent := `
<html>
	<body>
		<div class="target">
			<a href="/target/1">Link 1</a>
			<span><a href="/target/2">Link 2</a></span>
		</div>
		<div class="other">
			<a href="/other/1">Other 1</a>
		</div>
		<nav class="nav-list secondary">
			<a href="/nav/1">Nav 1</a>
		</nav>
	</body>
</html>
`
	tests := []struct {
		name     string
		classes  []string
		expected []string
	}{
		{
			"Single class",
			[]string{"target"},
			[]string{"/target/1", "/target/2"},
		},
		{
			"Multiple classes",
			[]string{"nav-list"},
			[]string{"/nav/1"},
		},
		{
			"No match",
			[]string{"nonexistent"},
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := strings.NewReader(htmlContent)
			result := app.extractLinksWithClassFilter(r, tt.classes)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("extractLinksWithClassFilter(%v) = %v; want %v", tt.classes, result, tt.expected)
			}
		})
	}
}

func TestEnqueueBatch(t *testing.T) {
	app := newTestApp("/allowed")
	app.processedURLs = make(map[string]bool)
	app.failedURLs = make(map[string]int)
	app.redirects = make(map[string]string)
	app.sessionQueued = make(map[string]bool)
	app.queueChan = make(chan string, 10)

	var wg sync.WaitGroup
	urls := []string{
		"https://docs.cloud.google.com/allowed/1",
		"https://docs.cloud.google.com/allowed/1", // Duplicate in same batch
		"https://cloud.google.com/allowed/1",      // Distinct corpus host
		"https://docs.cloud.google.com/blocked/1", // Wrong prefix
		"https://developers.google.com/allowed/1", // Path matches; allowed across hosts.
	}

	app.enqueueBatch(urls, &wg)

	if len(app.queueChan) != 3 {
		t.Fatalf("Expected 3 URLs in queue, got %d", len(app.queueChan))
	}
	got := make([]string, 0, 3)
	for len(got) < 3 {
		got = append(got, <-app.queueChan)
	}
	want := map[string]bool{
		"https://docs.cloud.google.com/allowed/1": true,
		"https://cloud.google.com/allowed/1":      true,
		"https://developers.google.com/allowed/1": true,
	}
	for _, u := range got {
		if !want[u] {
			t.Errorf("Unexpected URL in queue: %q", u)
		}
	}
	wg.Done()
	wg.Done()
	wg.Done()
	wg.Wait()
}

func TestEnqueueSeedsAllowsExplicitURLOutsidePrefixes(t *testing.T) {
	app := newTestApp("docs.cloud.google.com/spanner/")
	app.processedURLs = make(map[string]bool)
	app.failedURLs = make(map[string]int)
	app.redirects = make(map[string]string)
	app.sessionQueued = make(map[string]bool)
	app.queueChan = make(chan string, 2)

	const productURL = "https://cloud.google.com/spanner"
	var wg sync.WaitGroup
	app.enqueueSeeds([]string{productURL}, &wg)
	app.enqueueBatch([]string{"https://cloud.google.com/spanner/pricing"}, &wg)

	if got := len(app.queueChan); got != 1 {
		t.Fatalf("queued URLs = %d, want 1", got)
	}
	if got := <-app.queueChan; got != productURL {
		t.Errorf("queued URL = %q, want %q", got, productURL)
	}
	wg.Done()
	wg.Wait()
}

func TestDiskStorage_Save(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mirror-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	storage := &DiskStorage{docsDir: tmpDir}
	docs := []Document{
		{
			Name:       "documents/docs.cloud.google.com/test/page1",
			URI:        "https://docs.cloud.google.com/test/page1",
			Title:      "Page 1",
			UpdateTime: "2026-05-08T21:32:47Z",
			Content:    "Content 1",
		},
		{
			Name:       "documents/docs.cloud.google.com/test/page2",
			URI:        "https://docs.cloud.google.com/test/page2",
			Title:      "Page 2",
			UpdateTime: "2026-05-08T21:32:47Z",
			Content:    "Content 2\n",
		},
	}

	if err := storage.Save(docs...); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify file 1
	p1 := filepath.Join(tmpDir, "docs.cloud.google.com/test/page1.md")
	c1, err := os.ReadFile(p1)
	if err != nil {
		t.Fatal(err)
	}
	expected1 := "---\nname: documents/docs.cloud.google.com/test/page1\nuri: https://docs.cloud.google.com/test/page1\ntitle: Page 1\n---\n\nContent 1\n"
	if string(c1) != expected1 {
		t.Errorf("Expected %q, got %q", expected1, string(c1))
	}

	// Verify file 2
	p2 := filepath.Join(tmpDir, "docs.cloud.google.com/test/page2.md")
	c2, err := os.ReadFile(p2)
	if err != nil {
		t.Fatal(err)
	}
	expected2 := "---\nname: documents/docs.cloud.google.com/test/page2\nuri: https://docs.cloud.google.com/test/page2\ntitle: Page 2\n---\n\nContent 2\n"
	if string(c2) != expected2 {
		t.Errorf("Expected %q, got %q", expected2, string(c2))
	}
}

func TestDiskStorageLoadProcessedURLUpdateTimes(t *testing.T) {
	tmpDir := t.TempDir()
	logDir := filepath.Join(tmpDir, "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	content := strings.Join([]string{
		"https://docs.cloud.google.com/spanner/docs\t2026-05-08T21:32:47Z",
		"https://docs.cloud.google.com/spanner/docs/backup",
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join(logDir, "urls.txt"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	storage := &DiskStorage{logDir: logDir}
	got, err := storage.LoadProcessedURLUpdateTimes()
	if err != nil {
		t.Fatalf("LoadProcessedURLUpdateTimes() error = %v", err)
	}
	want := map[string]string{
		"https://docs.cloud.google.com/spanner/docs":        "2026-05-08T21:32:47Z",
		"https://docs.cloud.google.com/spanner/docs/backup": "",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadProcessedURLUpdateTimes() = %#v, want %#v", got, want)
	}
}

func TestSaveMetadataWritesURLLogWithUpdateTime(t *testing.T) {
	tmpDir := t.TempDir()
	app := &MirrorApp{
		cfg: &Config{
			LogDir:       filepath.Join(tmpDir, "logs"),
			DocsDir:      filepath.Join(tmpDir, "docs"),
			MetadataFile: filepath.Join(tmpDir, "metadata.yaml"),
		},
		processedURLs: map[string]bool{
			"https://docs.cloud.google.com/spanner/docs":        true,
			"https://docs.cloud.google.com/spanner/docs/backup": true,
		},
		updateTimes: map[string]string{
			"https://docs.cloud.google.com/spanner/docs":        "2026-05-08T21:32:47Z",
			"https://docs.cloud.google.com/spanner/docs/backup": "",
		},
		failedURLs: make(map[string]int),
		redirects:  make(map[string]string),
	}

	app.saveMetadata()

	data, err := os.ReadFile(filepath.Join(app.cfg.LogDir, "urls.txt"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	got := string(data)
	want := "" +
		"https://docs.cloud.google.com/spanner/docs\t2026-05-08T21:32:47Z\n" +
		"https://docs.cloud.google.com/spanner/docs/backup\t\n"
	if got != want {
		t.Fatalf("urls.txt = %q, want %q", got, want)
	}
}

func TestDefaultKnownHosts(t *testing.T) {
	hosts := defaultKnownHosts()
	required := []string{
		"cloud.google.com",
		"dart.dev",
		"docs.cloud.google.com",
		"docs.flutter.dev",
		"mapsplatform.google.com",
	}
	for _, h := range required {
		if !slices.Contains(hosts, h) {
			t.Errorf("defaultKnownHosts() missing %q", h)
		}
	}
}

func TestLimiterBurst(t *testing.T) {
	tests := []struct {
		qpm  float64
		want int
	}{
		{50, 8},
		{5, 5},
		{0.5, 1},
	}
	for _, tt := range tests {
		if got := limiterBurst(tt.qpm); got != tt.want {
			t.Errorf("limiterBurst(%v) = %d, want %d", tt.qpm, got, tt.want)
		}
	}
}
