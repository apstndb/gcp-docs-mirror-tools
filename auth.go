package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const (
	cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"
	apiHTTPTimeout     = time.Minute
)

var defaultTokenSource = func(ctx context.Context, scopes ...string) (oauth2.TokenSource, error) {
	return google.DefaultTokenSource(ctx, scopes...)
}

type adcCredentialsMetadata struct {
	Type           string `json:"type"`
	QuotaProjectID string `json:"quota_project_id"`
}

var adcCredentialsPath = func() string {
	if path := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); path != "" {
		return path
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = ""
	}
	return defaultADCCredentialsPath(runtime.GOOS, homeDir, os.Getenv("APPDATA"))
}

func defaultADCCredentialsPath(goos, homeDir, appData string) string {
	if goos == "windows" {
		if appData == "" {
			return ""
		}
		return filepath.Join(appData, "gcloud", "application_default_credentials.json")
	}
	if homeDir == "" {
		return ""
	}
	return filepath.Join(homeDir, ".config", "gcloud", "application_default_credentials.json")
}

func apiKeyFromEnv() string {
	if key := os.Getenv("DEVELOPERKNOWLEDGE_API_KEY"); key != "" {
		return key
	}
	if key := os.Getenv("GOOGLE_API_KEY"); key != "" {
		return key
	}
	return ""
}

func newDeveloperKnowledgeHTTPClient(ctx context.Context) (*http.Client, string, error) {
	if apiKey := apiKeyFromEnv(); apiKey != "" {
		return &http.Client{Timeout: apiHTTPTimeout}, apiKey, nil
	}

	tokenSource, err := defaultTokenSource(ctx, cloudPlatformScope)
	if err != nil {
		return nil, "", fmt.Errorf("set DEVELOPERKNOWLEDGE_API_KEY or GOOGLE_API_KEY, or configure ADC with 'gcloud auth application-default login': %w", err)
	}

	client := oauth2.NewClient(ctx, tokenSource)
	client.Timeout = apiHTTPTimeout

	quotaProject, metadata := resolveQuotaProjectID()
	if quotaProject == "" && metadata.Type == "authorized_user" {
		return nil, "", fmt.Errorf("ADC requires a quota project; run 'gcloud auth application-default set-quota-project <project-id>' or set GOOGLE_CLOUD_QUOTA_PROJECT")
	}
	if quotaProject != "" {
		baseTransport := client.Transport
		if baseTransport == nil {
			baseTransport = http.DefaultTransport
		}
		client.Transport = &quotaProjectTransport{
			Base:    baseTransport,
			Project: quotaProject,
		}
	}

	return client, "", nil
}

func loadADCCredentialsMetadata() adcCredentialsMetadata {
	path := adcCredentialsPath()
	if path == "" {
		return adcCredentialsMetadata{}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return adcCredentialsMetadata{}
	}
	var cfg adcCredentialsMetadata
	if err := json.Unmarshal(data, &cfg); err != nil {
		return adcCredentialsMetadata{}
	}
	return cfg
}

func resolveQuotaProjectID() (string, adcCredentialsMetadata) {
	if project := os.Getenv("GOOGLE_CLOUD_QUOTA_PROJECT"); project != "" {
		return project, adcCredentialsMetadata{}
	}

	cfg := loadADCCredentialsMetadata()
	return cfg.QuotaProjectID, cfg
}

type quotaProjectTransport struct {
	Base    http.RoundTripper
	Project string
}

func (t *quotaProjectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("X-Goog-User-Project", t.Project)
	return t.Base.RoundTrip(req)
}
