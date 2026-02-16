package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/yuin/goldmark"
)

func TestToRootRelative(t *testing.T) {
	app := &MirrorApp{}
	tests := []struct {
		input    string
		expected string
	}{
		{"https://docs.cloud.google.com/spanner/docs", "/spanner/docs"},
		{"https://cloud.google.com/spanner/docs/", "/spanner/docs"},
		{"https://docs.cloud.google.com/spanner/docs/dml-versus-mutations.md", "/spanner/docs/dml-versus-mutations"},
		{"http://docs.cloud.google.com/spanner/docs#anchor", "/spanner/docs"},
		{"/spanner/docs/backup/", "/spanner/docs/backup"},
		{"spanner/docs", "/spanner/docs"},
	}

	for _, tt := range tests {
		result := app.toRootRelative(tt.input)
		if result != tt.expected {
			t.Errorf("toRootRelative(%q) = %q; want %q", tt.input, result, tt.expected)
		}
	}
}

func TestResolveAndNormalize(t *testing.T) {
	app := &MirrorApp{}
	tests := []struct {
		link     string
		base     string
		expected string
	}{
		{"concepts", "/spanner/docs", "https://docs.cloud.google.com/spanner/docs/concepts"},
		{"backup/", "/spanner/docs", "https://docs.cloud.google.com/spanner/docs/backup"},
		{"dml-versus-mutations.md", "/spanner/docs", "https://docs.cloud.google.com/spanner/docs/dml-versus-mutations"},
		{"/spanner/docs/concepts/", "/irrelevant", "https://docs.cloud.google.com/spanner/docs/concepts"},
		{"https://cloud.google.com/spanner/docs/", "/any", "https://docs.cloud.google.com/spanner/docs"},
		{"#anchor", "/spanner/docs", "https://docs.cloud.google.com/spanner/docs"},
	}

	for _, tt := range tests {
		result := app.resolveAndNormalize(tt.link, tt.base)
		if result != tt.expected {
			t.Errorf("resolveAndNormalize(%q, %q) = %q; want %q", tt.link, tt.base, result, tt.expected)
		}
	}
}

func TestNormalizeForAPI(t *testing.T) {
	app := &MirrorApp{}
	tests := []struct {
		input    string
		expected string
	}{
		{"https://docs.cloud.google.com/spanner/docs", "documents/docs.cloud.google.com/spanner/docs"},
		{"https://cloud.google.com/spanner/docs", "documents/docs.cloud.google.com/spanner/docs"},
		{"cloud.google.com/spanner/docs", "documents/docs.cloud.google.com/spanner/docs"},
	}

	for _, tt := range tests {
		result := app.normalizeForAPI(tt.input)
		if result != tt.expected {
			t.Errorf("normalizeForAPI(%q) = %q; want %q", tt.input, result, tt.expected)
		}
	}
}

func TestMatchesAnyPrefix(t *testing.T) {
	app := &MirrorApp{
		cfg: &Config{
			Prefixes: []string{"/spanner/docs", "/sdk/gcloud/reference/spanner"},
		},
	}
	tests := []struct {
		input    string
		expected bool
	}{
		{"https://docs.cloud.google.com/spanner/docs/overview", true},
		{"https://docs.cloud.google.com/spanner/docs", true},
		{"https://docs.cloud.google.com/sdk/gcloud/reference/spanner", true},
		{"https://docs.cloud.google.com/sdk/gcloud/reference/spanner/describe", true},
		{"https://docs.cloud.google.com/bigtable/docs", false},
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
	app := &MirrorApp{
		cfg: &Config{
			Prefixes: []string{"/allowed"},
		},
		processedURLs: make(map[string]bool),
		failedURLs:    make(map[string]bool),
		redirects:     make(map[string]string),
		sessionQueued: make(map[string]bool),
		queueChan:     make(chan string, 10),
	}

	var wg sync.WaitGroup
	urls := []string{
		"https://docs.cloud.google.com/allowed/1",
		"https://docs.cloud.google.com/allowed/1", // Duplicate in same batch
		"https://docs.cloud.google.com/blocked/1", // Wrong prefix
	}

	app.enqueueBatch(urls, &wg)

	// Verify deduplication and prefix filtering
	if len(app.queueChan) != 1 {
		t.Errorf("Expected 1 URL in queue, got %d", len(app.queueChan))
	}

	// Verify WaitGroup was incremented correctly
	// We can't directly check the internal counter of WaitGroup, 
	// but we can try to call Done() and see if it's correct.
	// However, a better way is to see if we can read from the channel and then Wait().
	u := <-app.queueChan
	if u != "https://docs.cloud.google.com/allowed/1" {
		t.Errorf("Expected allowed/1, got %q", u)
	}
	wg.Done()
	wg.Wait() // Should not hang if Add(1) was called exactly once
}

func TestDiskStorage_Save(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mirror-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage := &DiskStorage{docsDir: tmpDir}
	docs := []Document{
		{Name: "documents/docs.cloud.google.com/test/page1", Content: "Content 1"},
		{Name: "documents/docs.cloud.google.com/test/page2", Content: "Content 2\n"},
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
	if string(c1) != "Content 1\n" { // Should have added newline
		t.Errorf("Expected 'Content 1\\n', got %q", string(c1))
	}

	// Verify file 2
	p2 := filepath.Join(tmpDir, "docs.cloud.google.com/test/page2.md")
	c2, err := os.ReadFile(p2)
	if err != nil {
		t.Fatal(err)
	}
	if string(c2) != "Content 2\n" { // Should not have doubled newline
		t.Errorf("Expected 'Content 2\\n', got %q", string(c2))
	}
}
