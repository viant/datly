package otel

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/net/http/httpguts"
)

// HTTPExporter configures the official OTLP/HTTP transport; it owns no queue or
// span mapping. The existing Adapter owns invocation batching and export lifetime.
type HTTPExporter struct {
	EndpointURL string
	Insecure    bool
	Headers     map[string]string
}

func (c HTTPExporter) Validate() error {
	u, err := url.Parse(c.EndpointURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || u.Scheme != "https" && u.Scheme != "http" {
		return fmt.Errorf("OTLP HTTP requires an absolute HTTP(S) EndpointURL without userinfo, query or fragment")
	}
	if (u.Scheme == "http") != c.Insecure {
		return fmt.Errorf("OTLP HTTP Insecure must explicitly match the endpoint scheme")
	}
	if u.Hostname() == "" {
		return fmt.Errorf("OTLP HTTP hostname is required")
	}
	if u.Port() != "" {
		port, err := strconv.Atoi(u.Port())
		if err != nil || port < 1 || port > 65535 {
			return fmt.Errorf("invalid OTLP HTTP port")
		}
	}
	seen := map[string]bool{}
	for name, value := range c.Headers {
		canonical := strings.ToLower(name)
		if seen[canonical] || !httpguts.ValidHeaderFieldName(name) || !httpguts.ValidHeaderFieldValue(value) || canonical == "content-type" || canonical == "content-length" || canonical == "transfer-encoding" || canonical == "host" {
			return fmt.Errorf("invalid OTLP HTTP header")
		}
		seen[canonical] = true
	}
	return nil
}

func (c HTTPExporter) New(ctx context.Context, timeout time.Duration) (sdktrace.SpanExporter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if timeout < 0 {
		return nil, fmt.Errorf("OTLP HTTP timeout must not be negative")
	}
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	headers := make(map[string]string, len(c.Headers))
	for name, value := range c.Headers {
		headers[name] = value
	}
	options := []otlptracehttp.Option{otlptracehttp.WithEndpointURL(c.EndpointURL), otlptracehttp.WithHeaders(headers), otlptracehttp.WithCompression(otlptracehttp.NoCompression), otlptracehttp.WithTimeout(timeout), otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false})}
	if c.Insecure {
		options = append(options, otlptracehttp.WithInsecure())
	} else {
		options = append(options, otlptracehttp.WithTLSClientConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	result, err := otlptracehttp.New(ctx, options...)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("initialize OTLP HTTP exporter failed")
	}
	return result, nil
}
