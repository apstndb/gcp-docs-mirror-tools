package main

import (
	"reflect"
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
