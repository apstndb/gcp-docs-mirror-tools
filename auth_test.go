package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dkapi "github.com/apstndb/developerknowledge-go"
	"golang.org/x/oauth2"
)

func TestNewDeveloperKnowledgeHTTPClientPrefersAPIKey(t *testing.T) {
	t.Setenv("DEVELOPERKNOWLEDGE_API_KEY", "test-key")
	t.Setenv("GOOGLE_API_KEY", "")

	oldTokenSource := defaultTokenSource
	defaultTokenSource = func(context.Context, ...string) (oauth2.TokenSource, error) {
		t.Fatal("defaultTokenSource should not be called when API key is set")
		return nil, nil
	}
	defer func() {
		defaultTokenSource = oldTokenSource
	}()

	client, apiKey, err := newDeveloperKnowledgeHTTPClient(context.Background())
	if err != nil {
		t.Fatalf("newDeveloperKnowledgeHTTPClient returned error: %v", err)
	}
	if apiKey != "test-key" {
		t.Fatalf("apiKey = %q, want %q", apiKey, "test-key")
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNewDeveloperKnowledgeHTTPClientRequiresQuotaProjectForAuthorizedUserADC(t *testing.T) {
	t.Setenv("DEVELOPERKNOWLEDGE_API_KEY", "")
	t.Setenv("GOOGLE_API_KEY", "")
	t.Setenv("GOOGLE_CLOUD_QUOTA_PROJECT", "")

	oldTokenSource := defaultTokenSource
	defaultTokenSource = func(context.Context, ...string) (oauth2.TokenSource, error) {
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test-token"}), nil
	}
	defer func() {
		defaultTokenSource = oldTokenSource
	}()

	tmpDir := t.TempDir()
	adcPath := filepath.Join(tmpDir, "application_default_credentials.json")
	if err := os.WriteFile(adcPath, []byte(`{"type":"authorized_user"}`), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	oldADCPath := adcCredentialsPath
	adcCredentialsPath = func() string { return adcPath }
	defer func() {
		adcCredentialsPath = oldADCPath
	}()

	_, _, err := newDeveloperKnowledgeHTTPClient(context.Background())
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "ADC requires a quota project") {
		t.Fatalf("error = %v, want quota project guidance", err)
	}
}

func TestNewDeveloperKnowledgeHTTPClientUsesADCQuotaProject(t *testing.T) {
	t.Setenv("DEVELOPERKNOWLEDGE_API_KEY", "")
	t.Setenv("GOOGLE_API_KEY", "")
	t.Setenv("GOOGLE_CLOUD_QUOTA_PROJECT", "test-project")

	oldTokenSource := defaultTokenSource
	defaultTokenSource = func(context.Context, ...string) (oauth2.TokenSource, error) {
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test-token"}), nil
	}
	defer func() {
		defaultTokenSource = oldTokenSource
	}()

	client, apiKey, err := newDeveloperKnowledgeHTTPClient(context.Background())
	if err != nil {
		t.Fatalf("newDeveloperKnowledgeHTTPClient returned error: %v", err)
	}
	if apiKey != "" {
		t.Fatalf("apiKey = %q, want empty string", apiKey)
	}
	if _, ok := client.Transport.(*dkapi.QuotaProjectTransport); !ok {
		t.Fatalf("client.Transport = %T, want *dkapi.QuotaProjectTransport", client.Transport)
	}
}
