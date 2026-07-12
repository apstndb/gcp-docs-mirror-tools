package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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

	client, apiKey, err := newDeveloperKnowledgeHTTPClientWithConfig(context.Background(), dkapi.AuthConfig{
		TokenSource: func(context.Context, ...string) (oauth2.TokenSource, error) {
			t.Fatal("defaultTokenSource should not be called when API key is set")
			return nil, nil
		},
	})
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

	tmpDir := t.TempDir()
	adcPath := filepath.Join(tmpDir, "application_default_credentials.json")
	if err := os.WriteFile(adcPath, []byte(`{
		"type":"authorized_user",
		"client_id":"test-client",
		"client_secret":"test-secret",
		"refresh_token":"test-refresh-token"
	}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, _, err := newDeveloperKnowledgeHTTPClientWithConfig(context.Background(), dkapi.AuthConfig{
		CredentialsPath: func() string { return adcPath },
	})
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

	requestHeaders := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requestHeaders <- r.Header.Clone():
		default:
			t.Error("unexpected additional request")
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	client, apiKey, err := newDeveloperKnowledgeHTTPClientWithConfig(context.Background(), dkapi.AuthConfig{
		AllowedOrigin: server.URL,
		TokenSource: func(context.Context, ...string) (oauth2.TokenSource, error) {
			return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test-token"}), nil
		},
	})
	if err != nil {
		t.Fatalf("newDeveloperKnowledgeHTTPClient returned error: %v", err)
	}
	if apiKey != "" {
		t.Fatalf("apiKey = %q, want empty string", apiKey)
	}
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("client.Get() error = %v", err)
	}
	_ = resp.Body.Close()
	if got := (<-requestHeaders).Get("x-goog-user-project"); got != "test-project" {
		t.Fatalf("x-goog-user-project = %q, want %q", got, "test-project")
	}
}
