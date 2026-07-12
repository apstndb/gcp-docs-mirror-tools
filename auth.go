package main

import (
	"context"
	"net/http"
	"time"

	"github.com/apstndb/developerknowledge-go"
)

const (
	apiHTTPTimeout = time.Minute
)

func newDeveloperKnowledgeHTTPClient(ctx context.Context) (*http.Client, string, error) {
	return newDeveloperKnowledgeHTTPClientWithConfig(ctx, dkapi.AuthConfig{})
}

func newDeveloperKnowledgeHTTPClientWithConfig(ctx context.Context, cfg dkapi.AuthConfig) (*http.Client, string, error) {
	cfg.Mode = dkapi.AuthPreferAPIKey
	cfg.Timeout = apiHTTPTimeout
	return dkapi.NewAuthenticatedHTTPClient(ctx, cfg)
}
