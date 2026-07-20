package main

import (
	"context"
	"net/http"

	"github.com/apstndb/developerknowledge-go"
)

func newDeveloperKnowledgeHTTPClient(ctx context.Context) (*http.Client, string, error) {
	return newDeveloperKnowledgeHTTPClientWithConfig(ctx, dkapi.AuthConfig{})
}

func newDeveloperKnowledgeHTTPClientWithConfig(ctx context.Context, cfg dkapi.AuthConfig) (*http.Client, string, error) {
	cfg.Mode = dkapi.AuthPreferAPIKey
	cfg.Timeout = dkapi.DefaultHTTPTimeout
	return dkapi.NewAuthenticatedHTTPClient(ctx, cfg)
}
