package main

import (
	"context"
	"net/http"
	"time"

	"github.com/apstndb/developerknowledge-go"
	"golang.org/x/oauth2"
)

const (
	cloudPlatformScope = dkapi.CloudPlatformScope
	apiHTTPTimeout     = time.Minute
)

var defaultTokenSource = func(ctx context.Context, scopes ...string) (oauth2.TokenSource, error) {
	return dkapi.DefaultTokenSource(ctx, scopes...)
}

type adcCredentialsMetadata = dkapi.ADCCredentialsMetadata

var adcCredentialsPath = dkapi.DefaultCredentialsPath

func defaultADCCredentialsPath(goos, homeDir, appData string) string {
	return dkapi.DefaultADCCredentialsPath(goos, homeDir, appData)
}

func apiKeyFromEnv() string {
	return dkapi.APIKeyFromEnv()
}

func newDeveloperKnowledgeHTTPClient(ctx context.Context) (*http.Client, string, error) {
	return dkapi.NewAuthenticatedHTTPClient(ctx, dkapi.AuthConfig{
		Mode:            dkapi.AuthPreferAPIKey,
		Timeout:         apiHTTPTimeout,
		TokenSource:     defaultTokenSource,
		CredentialsPath: adcCredentialsPath,
	})
}

func loadADCCredentialsMetadata() adcCredentialsMetadata {
	return dkapi.LoadADCCredentialsMetadata(adcCredentialsPath)
}

func resolveQuotaProjectID() (string, adcCredentialsMetadata) {
	return dkapi.ResolveQuotaProjectID(adcCredentialsPath)
}

type quotaProjectTransport = dkapi.QuotaProjectTransport
