package main

import (
	"context"
	"net/http"
	"time"

	"github.com/apstndb/developerknowledge-go"
	"golang.org/x/oauth2"
)

const (
	apiHTTPTimeout = time.Minute
)

var defaultTokenSource = func(ctx context.Context, scopes ...string) (oauth2.TokenSource, error) {
	return dkapi.DefaultTokenSource(ctx, scopes...)
}

var adcCredentialsPath = dkapi.DefaultCredentialsPath

func newDeveloperKnowledgeHTTPClient(ctx context.Context) (*http.Client, string, error) {
	return dkapi.NewAuthenticatedHTTPClient(ctx, dkapi.AuthConfig{
		Mode:            dkapi.AuthPreferAPIKey,
		Timeout:         apiHTTPTimeout,
		TokenSource:     defaultTokenSource,
		CredentialsPath: adcCredentialsPath,
	})
}
