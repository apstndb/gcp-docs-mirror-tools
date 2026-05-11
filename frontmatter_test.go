package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/yuin/goldmark"
	"gopkg.in/yaml.v3"
)

func TestFormatDocumentForStorage(t *testing.T) {
	doc := Document{
		Name:        "documents/docs.cloud.google.com/spanner/docs",
		URI:         "https://docs.cloud.google.com/spanner/docs",
		Title:       `Spanner: "Overview"`,
		Description: "Line 1\nLine 2",
		DataSource:  "docs.cloud.google.com",
		UpdateTime:  "2026-05-08T21:32:47Z",
		Content:     "# Spanner\n",
	}

	got, err := formatDocumentForStorage(doc, false)
	if err != nil {
		t.Fatalf("formatDocumentForStorage() error = %v", err)
	}

	parts := strings.SplitN(got, "---\n", 3)
	if len(parts) != 3 {
		t.Fatalf("formatted content missing frontmatter fences: %q", got)
	}

	var meta map[string]string
	if err := yaml.Unmarshal([]byte(parts[1]), &meta); err != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", err)
	}

	wantMeta := map[string]string{
		"name":        "documents/docs.cloud.google.com/spanner/docs",
		"uri":         "https://docs.cloud.google.com/spanner/docs",
		"title":       `Spanner: "Overview"`,
		"description": "Line 1\nLine 2",
		"data_source": "docs.cloud.google.com",
	}
	if !reflect.DeepEqual(meta, wantMeta) {
		t.Fatalf("frontmatter = %#v, want %#v", meta, wantMeta)
	}
	if !strings.HasSuffix(got, "\n") {
		t.Fatalf("formatted content must end with newline: %q", got)
	}
	if !strings.Contains(got, "\n# Spanner\n") {
		t.Fatalf("formatted content missing body: %q", got)
	}
}

func TestFormatDocumentForStorageIncludesUpdateTimeWhenEnabled(t *testing.T) {
	doc := Document{
		Name:       "documents/docs.cloud.google.com/spanner/docs",
		URI:        "https://docs.cloud.google.com/spanner/docs",
		UpdateTime: "2026-05-08T21:32:47Z",
		Content:    "# Spanner\n",
	}

	got, err := formatDocumentForStorage(doc, true)
	if err != nil {
		t.Fatalf("formatDocumentForStorage() error = %v", err)
	}
	if !strings.Contains(got, "update_time: \"2026-05-08T21:32:47Z\"\n") {
		t.Fatalf("formatted content missing update_time: %q", got)
	}
}

func TestExtractLinksFromMarkdownIgnoresFrontmatter(t *testing.T) {
	app := &MirrorApp{
		mdParser: goldmark.New(),
	}
	source := []byte(`---
name: documents/docs.cloud.google.com/spanner/docs
uri: https://docs.cloud.google.com/spanner/docs
title: Spanner documentation
---

[Body Link](https://docs.cloud.google.com/spanner/docs/backup)
`)

	got := app.extractLinksFromMarkdown(source)
	want := []string{"https://docs.cloud.google.com/spanner/docs/backup"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("extractLinksFromMarkdown() = %v, want %v", got, want)
	}
}

func TestDocumentUnmarshalV1Metadata(t *testing.T) {
	var doc Document
	payload := []byte(`{
		"name": "documents/docs.cloud.google.com/spanner/docs",
		"uri": "https://docs.cloud.google.com/spanner/docs",
		"title": "Spanner documentation",
		"description": "A managed relational database service.",
		"dataSource": "docs.cloud.google.com",
		"updateTime": "2026-05-08T21:32:47Z",
		"view": "DOCUMENT_VIEW_CONTENT",
		"content": "# Spanner"
	}`)

	if err := json.Unmarshal(payload, &doc); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	want := Document{
		Name:        "documents/docs.cloud.google.com/spanner/docs",
		URI:         "https://docs.cloud.google.com/spanner/docs",
		Title:       "Spanner documentation",
		Description: "A managed relational database service.",
		DataSource:  "docs.cloud.google.com",
		UpdateTime:  "2026-05-08T21:32:47Z",
		View:        "DOCUMENT_VIEW_CONTENT",
		Content:     "# Spanner",
	}
	if !reflect.DeepEqual(doc, want) {
		t.Fatalf("document = %#v, want %#v", doc, want)
	}
}
