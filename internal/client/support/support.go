// Package support holds helpers shared by the default HTTP and MCP client
// implementations: option identity, header canonicalization, endpoint
// validation, limit resolution and the hardened transport factory.
package support

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/viant/xdatly/client"
)

// Identity digests the complete typed options so clients are scoped by
// configuration, never by caller.
func Identity(options any) (string, error) {
	encoded, err := json.Marshal(options)
	if err != nil {
		return "", fmt.Errorf("encode client options: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// CanonicalHeaders canonicalizes static header names and rejects duplicates.
func CanonicalHeaders(source map[string]string) (map[string]string, error) {
	result := make(map[string]string, len(source))
	for name, value := range source {
		canonical := http.CanonicalHeaderKey(strings.TrimSpace(name))
		if canonical == "" {
			return nil, fmt.Errorf("header name is required")
		}
		if _, ok := result[canonical]; ok {
			return nil, fmt.Errorf("header %q is declared more than once", canonical)
		}
		result[canonical] = value
	}
	return result, nil
}

// ValidateEndpointURL accepts absolute http/https URLs with a host and without
// embedded user information.
func ValidateEndpointURL(raw string) (*url.URL, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("url is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("url %q must use http or https", raw)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("url %q requires a host", raw)
	}
	if parsed.User != nil {
		return nil, fmt.Errorf("url %q must not embed credentials", raw)
	}
	return parsed, nil
}

// Limits validates explicit bounds. Omitted and zero values mean unlimited.
func Limits(limits client.Limits) (time.Duration, int64, error) {
	var timeout time.Duration
	if limits.Timeout != "" {
		parsed, err := time.ParseDuration(limits.Timeout)
		if err != nil {
			return 0, 0, fmt.Errorf("timeout: %w", err)
		}
		if parsed < 0 {
			return 0, 0, fmt.Errorf("timeout %s must not be negative", limits.Timeout)
		}
		timeout = parsed
	}
	if limits.MaxResponseBytes < 0 {
		return 0, 0, fmt.Errorf("maxResponseBytes %d must not be negative", limits.MaxResponseBytes)
	}
	return timeout, limits.MaxResponseBytes, nil
}

// WithTimeout preserves caller cancellation without imposing a deadline at zero.
func WithTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

// NewTransportClient keeps default TLS verification and never follows
// redirects, so forwarded headers cannot leak to a redirect target.
func NewTransportClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
