package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yuin/goldmark"
)

func TestRecursiveFetchDiscoversFreshContent(t *testing.T) {
	for _, tc := range []struct {
		recursive bool
		prefix    string
	}{
		{false, "docs.cloud.google.com/test/"},
		{true, "docs.cloud.google.com/test/"},
		{true, "/"},
		{true, "docs.cloud.google.com/"},
	} {
		t.Run(fmt.Sprintf("%t/%s", tc.recursive, tc.prefix), func(t *testing.T) {
			var mu sync.Mutex
			seen := map[string]int{}
			app := newFetchTestApp(mirrorRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
				var docs []Document
				for _, name := range req.URL.Query()["names"] {
					mu.Lock()
					seen[name]++
					mu.Unlock()
					content := "[cycle](https://docs.cloud.google.com/test/root)"
					if strings.HasSuffix(name, "/root") {
						// Exceed both the queue capacity and number of workers.
						for i := range 700 {
							content += fmt.Sprintf("\n[child](https://docs.cloud.google.com/test/child%d)", i)
						}
						content += "\n[outside](https://docs.cloud.google.com/other/page)"
						content += "\n[external](https://example.com/unknown)"
					} else if strings.HasSuffix(name, "/child0") {
						content += "\n[grandchild](https://docs.cloud.google.com/test/grandchild)"
					}
					docs = append(docs, Document{Name: name, Content: content})
				}
				body, err := json.Marshal(map[string]any{"documents": docs})
				if err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
			}))
			app.cfg.Recursive = tc.recursive
			app.cfg.DocsDir = t.TempDir()
			app.cfg.MetadataFile = filepath.Join(app.cfg.DocsDir, "metadata.yaml")
			app.cfg.Prefixes = []string{tc.prefix}
			app.prefixRules = app.parsePrefixes(app.cfg.Prefixes)
			app.storage = stubStorage{}
			app.mdParser = goldmark.New()
			app.queueChan = make(chan string, 1)
			app.sessionQueued = make(map[string]bool)
			done := make(chan error, 1)
			go func() { done <- app.Run(context.Background(), []string{"https://docs.cloud.google.com/test/root"}) }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("recursive discovery deadlocked")
			}
			want := 1
			if tc.recursive {
				want = 702
				if tc.prefix != "docs.cloud.google.com/test/" {
					want++ // The same-host /other/page link is in scope.
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if len(seen) != want {
				t.Fatalf("fetched %d unique documents, want %d", len(seen), want)
			}
			for name, count := range seen {
				if count != 1 || (tc.prefix == "docs.cloud.google.com/test/" && strings.Contains(name, "/other/")) || name == "documents/docs.cloud.google.com/" {
					t.Errorf("unexpected fetch: %s (%d times)", name, count)
				}
			}
		})
	}
}
