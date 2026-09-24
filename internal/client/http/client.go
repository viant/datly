// Package http is Datly's default implementation of the public outbound HTTP
// client contract (github.com/viant/xdatly/client/http). One Client is built
// per distinct typed Options value; it applies the configured static headers,
// timeout and response-size bound to every explicit request it executes.
package http

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/viant/datly/internal/client/support"
	xhttp "github.com/viant/xdatly/client/http"
)

// Validate checks the complete declaration without opening any connection.
func Validate(options xhttp.Options) error {
	if _, err := support.ValidateEndpointURL(options.URL); err != nil {
		return fmt.Errorf("http url: %w", err)
	}
	switch options.Method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		return fmt.Errorf("http method %q must be one of GET, POST, PUT, PATCH, DELETE", options.Method)
	}
	if _, err := support.CanonicalHeaders(options.Header); err != nil {
		return fmt.Errorf("http header: %w", err)
	}
	if _, _, err := support.Limits(options.Limits); err != nil {
		return fmt.Errorf("http %w", err)
	}
	return nil
}

// Client implements xhttp.Client for one configured destination.
type Client struct {
	identity string
	scheme   string
	host     string
	header   map[string]string
	timeout  time.Duration
	maxBytes int64
	client   *http.Client
}

// New validates options and builds the client.
func New(options xhttp.Options) (*Client, error) {
	if err := Validate(options); err != nil {
		return nil, err
	}
	identity, err := support.Identity(options)
	if err != nil {
		return nil, err
	}
	endpoint, _ := support.ValidateEndpointURL(options.URL)
	header, _ := support.CanonicalHeaders(options.Header)
	timeout, maxBytes, _ := support.Limits(options.Limits)
	return &Client{identity: identity, scheme: endpoint.Scheme, host: endpoint.Host, header: header, timeout: timeout, maxBytes: maxBytes, client: support.NewTransportClient()}, nil
}

// Identity is the digest of the options the client was built from.
func (c *Client) Identity() string { return c.identity }

// Do executes one explicit request against the configured destination. Static
// headers fill in only names the request did not set; the request context is
// bounded by the configured timeout, and the response body errors once it
// exceeds the configured byte limit.
func (c *Client) Do(request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil {
		return nil, fmt.Errorf("http request is required")
	}
	if !strings.EqualFold(request.URL.Scheme, c.scheme) || !strings.EqualFold(request.URL.Host, c.host) {
		return nil, fmt.Errorf("request %s://%s does not target the configured endpoint %s://%s", request.URL.Scheme, request.URL.Host, c.scheme, c.host)
	}
	ctx, cancel := support.WithTimeout(request.Context(), c.timeout)
	request = request.Clone(ctx)
	for name, value := range c.header {
		if _, present := request.Header[name]; !present {
			request.Header.Set(name, value)
		}
	}
	response, err := c.client.Do(request)
	if err != nil {
		// Capture cancellation before cleanup cancels the derived context;
		// otherwise every transport failure is misreported as cancellation.
		ctxErr := ctx.Err()
		cancel()
		if ctxErr != nil {
			return nil, fmt.Errorf("remote %s %s: %w", request.Method, redact(request.URL), ctxErr)
		}
		return nil, fmt.Errorf("remote %s %s: %w", request.Method, redact(request.URL), err)
	}
	var reader io.Reader = response.Body
	if c.maxBytes > 0 && c.maxBytes < int64(^uint64(0)>>1) {
		reader = io.LimitReader(response.Body, c.maxBytes+1)
	}
	response.Body = &boundedBody{reader: reader, closer: response.Body, limit: c.maxBytes, cancel: cancel}
	return response, nil
}

// Close releases idle connections. Only the provider owner calls it.
func (c *Client) Close() error {
	c.client.CloseIdleConnections()
	return nil
}

func redact(target *url.URL) string {
	copied := *target
	copied.RawQuery = ""
	copied.User = nil
	return copied.String()
}

// boundedBody fails the read once the response exceeds the configured limit
// and releases the request timeout when the body is closed.
type boundedBody struct {
	reader io.Reader
	closer io.Closer
	limit  int64
	read   int64
	cancel context.CancelFunc
}

func (b *boundedBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += int64(n)
	if b.limit > 0 && b.read > b.limit {
		return 0, fmt.Errorf("response exceeds %d bytes", b.limit)
	}
	return n, err
}

func (b *boundedBody) Close() error {
	defer b.cancel()
	return b.closer.Close()
}
