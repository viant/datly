// Package mcp is Datly's default implementation of the public outbound MCP
// client contract (github.com/viant/xdatly/client/mcp) built on
// github.com/viant/mcp. Sessions are scoped by the exact header set they were
// initialized with, so principals never share a session and every HTTP stage
// of a session carries the same explicit headers.
package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/viant/datly/internal/client/support"
	"github.com/viant/jsonrpc/transport/client/http/streamable"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp/client"
	xmcp "github.com/viant/xdatly/client/mcp"
)

const (
	// TransportStreamable is the supported viant/mcp HTTP transport.
	TransportStreamable = "streamable"
)

// Validate checks the complete declaration without opening any connection.
func Validate(options xmcp.Options) error {
	if _, err := support.ValidateEndpointURL(options.URL); err != nil {
		return fmt.Errorf("mcp url: %w", err)
	}
	if strings.TrimSpace(options.Tool) == "" {
		return fmt.Errorf("mcp tool is required")
	}
	switch options.Transport {
	case "", TransportStreamable:
	default:
		return fmt.Errorf("mcp transport %q must be %q", options.Transport, TransportStreamable)
	}
	if _, err := support.CanonicalHeaders(options.Header); err != nil {
		return fmt.Errorf("mcp header: %w", err)
	}
	if options.MaxSessions < 0 {
		return fmt.Errorf("mcp maxSessions must be positive")
	}
	if _, _, err := support.Limits(options.Limits); err != nil {
		return fmt.Errorf("mcp %w", err)
	}
	return nil
}

// Client implements xmcp.Client for one configured endpoint and tool.
type Client struct {
	identity    string
	url         string
	tool        string
	protocol    string
	header      map[string]string
	timeout     time.Duration
	maxBytes    int64
	maxSessions int
	mu          sync.Mutex
	sessions    map[string]*session
	closed      bool
}

type session struct {
	mu       sync.Mutex
	closed   bool
	header   http.Header
	client   *client.Client
	lastUsed time.Time
}

// New validates options and builds the client. No connection is opened until
// the first call.
func New(options xmcp.Options) (*Client, error) {
	if err := Validate(options); err != nil {
		return nil, err
	}
	identity, err := support.Identity(options)
	if err != nil {
		return nil, err
	}
	header, _ := support.CanonicalHeaders(options.Header)
	timeout, maxBytes, _ := support.Limits(options.Limits)
	protocol := strings.TrimSpace(options.ProtocolVersion)
	if protocol == "" {
		protocol = schema.LatestProtocolVersion
	}
	maxSessions := options.MaxSessions
	return &Client{identity: identity, url: options.URL, tool: options.Tool, protocol: protocol, header: header, timeout: timeout, maxBytes: maxBytes, maxSessions: maxSessions, sessions: map[string]*session{}}, nil
}

// Identity is the digest of the options the client was built from.
func (c *Client) Identity() string { return c.identity }

// Close releases every session. Only the provider owner calls it.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for key, active := range c.sessions {
		active.close()
		delete(c.sessions, key)
	}
	return nil
}

// Sessions reports the number of live header-scoped sessions.
func (c *Client) Sessions() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.sessions)
}

// CallTool invokes the configured tool. The request's tool name must match
// the configuration; the native result, including isError, is returned
// unchanged and protocol errors are returned as errors.
func (c *Client) CallTool(ctx context.Context, request *schema.CallToolRequest, options xmcp.CallOptions) (*schema.CallToolResult, error) {
	if request == nil {
		return nil, fmt.Errorf("mcp tool request is required")
	}
	if request.Params.Name != "" && request.Params.Name != c.tool {
		return nil, fmt.Errorf("mcp tool %q does not match configured tool %q", request.Params.Name, c.tool)
	}
	ctx, cancel := support.WithTimeout(ctx, c.timeout)
	defer cancel()
	ctx, cancelFailure := context.WithCancelCause(ctx)
	defer cancelFailure(nil)
	ctx = support.WithResponseLimitFailure(ctx, func(err error) { cancelFailure(err) })
	active, err := c.session(c.sessionHeader(options.Header), time.Now())
	if err != nil {
		return nil, err
	}
	native, err := c.connect(ctx, active)
	if err != nil {
		return nil, err
	}
	params := request.Params
	params.Name = c.tool
	result, err := native.CallTool(ctx, &params)
	if err != nil {
		if ctxErr := context.Cause(ctx); ctxErr != nil {
			c.reset(active, native)
			return nil, fmt.Errorf("mcp tool %s at %s: %w", c.tool, c.url, ctxErr)
		}
		return nil, fmt.Errorf("mcp tool %s at %s: %w", c.tool, c.url, err)
	}
	if result == nil {
		return nil, fmt.Errorf("mcp tool %s at %s returned no result", c.tool, c.url)
	}
	return result, nil
}

// sessionHeader merges static client headers with the explicit call headers;
// explicit values replace static ones by name.
func (c *Client) sessionHeader(header http.Header) http.Header {
	result := http.Header{}
	for name, value := range c.header {
		result.Set(name, value)
	}
	for name, values := range header {
		result.Del(name)
		for _, value := range values {
			result.Add(name, value)
		}
	}
	return result
}

// SessionKey identifies a session by its complete header set. JSON orders map
// keys deterministically while preserving value order and boundaries, so
// reordered or merged repeated headers never reuse another session.
func SessionKey(header http.Header) string {
	encoded, _ := json.Marshal(header) // string keys and values cannot fail
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func (c *Client) session(header http.Header, now time.Time) (*session, error) {
	key := SessionKey(header)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, fmt.Errorf("mcp client for %s is closed", c.url)
	}
	if existing, ok := c.sessions[key]; ok {
		existing.lastUsed = now
		return existing, nil
	}
	for c.maxSessions > 0 && len(c.sessions) >= c.maxSessions {
		var oldestKey string
		var oldest *session
		for candidate, active := range c.sessions {
			if oldest == nil || active.lastUsed.Before(oldest.lastUsed) {
				oldestKey, oldest = candidate, active
			}
		}
		oldest.close()
		delete(c.sessions, oldestKey)
	}
	created := &session{header: header.Clone(), lastUsed: now}
	c.sessions[key] = created
	return created, nil
}

func (c *Client) connect(ctx context.Context, active *session) (*client.Client, error) {
	active.mu.Lock()
	defer active.mu.Unlock()
	if active.closed {
		return nil, fmt.Errorf("mcp session is closed")
	}
	if active.client != nil {
		return active.client, nil
	}
	type outcome struct {
		client *client.Client
		err    error
	}
	done := make(chan outcome, 1)
	header := active.header.Clone()
	// The session outlives this request, so its streams must not inherit the
	// request cancellation; the request deadline still bounds the handshake.
	go func() {
		created, err := c.dial(context.WithoutCancel(ctx), header)
		done <- outcome{client: created, err: err}
	}()
	select {
	case <-ctx.Done():
		go func() {
			if result := <-done; result.client != nil {
				result.client.Close()
			}
		}()
		return nil, fmt.Errorf("connect mcp %s: %w", c.url, ctx.Err())
	case result := <-done:
		if result.err != nil {
			return nil, fmt.Errorf("connect mcp %s: %w", c.url, result.err)
		}
		active.client = result.client
		return active.client, nil
	}
}

// dial builds the viant/mcp client over the library's streamable transport.
// The header provider runs for every HTTP request of the session, so the
// explicit headers reach initialize/discovery and tools/call alike.
func (c *Client) dial(ctx context.Context, header http.Header) (*client.Client, error) {
	options := []streamable.Option{
		streamable.WithProtocolVersion(c.protocol),
		streamable.WithHTTPClient(support.NewBoundedTransportClient(c.maxBytes)),
		streamable.WithHandler(client.NewHandler(nil)),
		streamable.WithRequestHeaderProvider(func(_ context.Context, body []byte, target http.Header) error {
			target.Set("Accept", "application/json, text/event-stream")
			for name, values := range header {
				target.Del(name)
				for _, value := range values {
					target.Add(name, value)
				}
			}
			return applyStandardHeaders(target, body)
		}),
	}
	if c.protocol == schema.LatestProtocolVersion {
		options = append(options, streamable.WithStateless(), streamable.WithRunTimeout(0))
	}
	transport, err := streamable.New(ctx, c.url, options...)
	if err != nil {
		return nil, fmt.Errorf("create streamable transport: %w", err)
	}
	native := client.New("datly-remote", "1.0", transport, client.WithProtocolVersion(c.protocol), client.WithPingInterval(60*time.Second))
	handshake, cancel := support.WithTimeout(ctx, c.timeout)
	defer cancel()
	if _, err := native.Initialize(handshake); err != nil {
		native.Close()
		return nil, err
	}
	return native, nil
}

// applyStandardHeaders mirrors the protocol-owned routing headers of the
// streamable transport for requests that carry an id.
func applyStandardHeaders(header http.Header, body []byte) error {
	var request struct {
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
		ID     json.RawMessage `json:"id"`
	}
	if len(body) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, &request); err != nil {
		return fmt.Errorf("decode MCP request for standard headers: %w", err)
	}
	if request.Method == "" || len(request.ID) == 0 || string(request.ID) == "null" {
		return nil
	}
	header.Set(schema.HeaderMethod, request.Method)
	field := ""
	switch request.Method {
	case schema.MethodToolsCall, schema.MethodPromptsGet:
		field = "name"
	case schema.MethodResourcesRead:
		field = "uri"
	}
	if field == "" || len(request.Params) == 0 {
		return nil
	}
	var params map[string]json.RawMessage
	if err := json.Unmarshal(request.Params, &params); err != nil {
		return fmt.Errorf("decode %s params: %w", request.Method, err)
	}
	var name string
	if value := params[field]; len(value) > 0 {
		if err := json.Unmarshal(value, &name); err != nil {
			return fmt.Errorf("decode %s params.%s: %w", request.Method, field, err)
		}
	}
	if name != "" {
		header.Set(schema.HeaderName, name)
	}
	return nil
}

func (c *Client) reset(active *session, failed *client.Client) {
	active.mu.Lock()
	defer active.mu.Unlock()
	if active.client == failed && failed != nil {
		failed.Close()
		active.client = nil
	}
}

func (s *session) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
}
