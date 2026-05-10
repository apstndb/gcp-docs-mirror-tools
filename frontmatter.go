package main

import (
	"bytes"
	"strings"

	"gopkg.in/yaml.v3"
)

type documentFrontmatter struct {
	Name        string `yaml:"name"`
	URI         string `yaml:"uri,omitempty"`
	Title       string `yaml:"title,omitempty"`
	Description string `yaml:"description,omitempty"`
	DataSource  string `yaml:"data_source,omitempty"`
	UpdateTime  string `yaml:"update_time,omitempty"`
}

func formatDocumentForStorage(doc Document) (string, error) {
	meta := documentFrontmatter{
		Name:        doc.Name,
		URI:         doc.URI,
		Title:       doc.Title,
		Description: doc.Description,
		DataSource:  doc.DataSource,
		UpdateTime:  doc.UpdateTime,
	}
	buf, err := yaml.Marshal(meta)
	if err != nil {
		return "", err
	}

	body := strings.TrimRight(doc.Content, " \t\r\n")
	var sb strings.Builder
	sb.WriteString("---\n")
	sb.Write(buf)
	sb.WriteString("---\n")
	if body != "" {
		sb.WriteString("\n")
		sb.WriteString(body)
	}
	sb.WriteString("\n")
	return sb.String(), nil
}

func stripLeadingFrontmatter(source []byte) []byte {
	const fence = "---\n"
	if !bytes.HasPrefix(source, []byte(fence)) {
		return source
	}

	end := bytes.Index(source[len(fence):], []byte("\n---\n"))
	if end < 0 {
		return source
	}

	trimmed := source[len(fence)+end+len("\n---\n"):]
	return bytes.TrimLeft(trimmed, "\r\n")
}
