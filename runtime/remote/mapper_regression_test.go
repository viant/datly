package remote

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/viant/mcp-protocol/schema"
	xhttp "github.com/viant/xdatly/client/http"
	xmcp "github.com/viant/xdatly/client/mcp"
)

type regressionHTTPProvider struct {
	calls  atomic.Int64
	client xhttp.Client
	check  func(xhttp.Options)
}

func (p *regressionHTTPProvider) Client(_ context.Context, options xhttp.Options) (xhttp.Client, error) {
	p.calls.Add(1)
	if p.check != nil {
		p.check(options)
	}
	return p.client, nil
}

type regressionHTTPClient struct {
	do func(*http.Request) (*http.Response, error)
}

func (c *regressionHTTPClient) Do(request *http.Request) (*http.Response, error) {
	return c.do(request)
}

type regressionMCPProvider struct {
	calls  atomic.Int64
	client xmcp.Client
}

func (p *regressionMCPProvider) Client(context.Context, xmcp.Options) (xmcp.Client, error) {
	p.calls.Add(1)
	return p.client, nil
}

type regressionMCPClient struct {
	call func(context.Context, *schema.CallToolRequest, xmcp.CallOptions) (*schema.CallToolResult, error)
}

func (c *regressionMCPClient) CallTool(ctx context.Context, request *schema.CallToolRequest, options xmcp.CallOptions) (*schema.CallToolResult, error) {
	return c.call(ctx, request, options)
}

func regressionHTTPResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}
}

func regressionHTTPConfig() Config {
	return Config{
		Client:   ClientOptions{Transport: TransportHTTP, HTTP: &xhttp.Options{URL: "https://example.test/value", Method: http.MethodGet}},
		Response: Response{Mappings: []ResponseMapping{{Path: "/value", Output: "Value"}}},
	}
}

func regressionMCPConfig() Config {
	return Config{
		Client:   ClientOptions{Transport: TransportMCP, MCP: &xmcp.Options{URL: "https://example.test/mcp", Tool: "lookup"}},
		Response: Response{Mappings: []ResponseMapping{{Path: "/value", Output: "Value"}}},
	}
}

func TestMapperRegressionHTTPExplicitMappingsAndCredential(t *testing.T) {
	type input struct{ ID, Query, Tenant, Payload string }
	type output struct{ Value string }
	bodyPath := "/profile/name"
	config := regressionHTTPConfig()
	config.Client.HTTP.URL = "https://example.test/items/{id}?fixed=1"
	config.Client.HTTP.Method = http.MethodPost
	config.Client.HTTP.Header = map[string]string{"X-Static": "deployment"}
	config.Request = []RequestMapping{
		{Input: "ID", Path: "id"},
		{Input: "Query", Query: "q"},
		{Input: "Tenant", Header: "x-tenant"},
		{Input: "Payload", Body: &bodyPath},
	}
	config.Credential = &Credential{Resource: "secret", Header: "X-Service-Key"}
	provider := &regressionHTTPProvider{
		check: func(options xhttp.Options) {
			if options.Header["X-Static"] != "deployment" {
				t.Fatalf("configured static header was not passed to provider: %#v", options.Header)
			}
		},
		client: &regressionHTTPClient{do: func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodPost || request.URL.EscapedPath() != "/items/a%2Fb" || request.URL.Query().Get("fixed") != "1" || request.URL.Query().Get("q") != "hello world" {
				t.Fatalf("mapped URL/method: %s %s", request.Method, request.URL.String())
			}
			if request.Header.Get("X-Tenant") != "tenant-a" || request.Header.Get("X-Service-Key") != "Bearer server-owned" || request.Header.Get("Content-Type") != "application/json" {
				t.Fatalf("mapped headers: %#v", request.Header)
			}
			data, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			var body map[string]any
			if err := json.Unmarshal(data, &body); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(body, map[string]any{"profile": map[string]any{"name": "Ada"}}) {
				t.Fatalf("mapped body: %#v", body)
			}
			return regressionHTTPResponse(http.StatusOK, `{"value":"ok"}`), nil
		}},
	}
	mapper := NewMapper(WithResources(fstest.MapFS{"secret": &fstest.MapFile{Data: []byte("  Bearer server-owned\n")}}))
	var result output
	if err := mapper.HTTP(context.Background(), &config, provider, nil, &input{ID: "a/b", Query: "hello world", Tenant: "tenant-a", Payload: "Ada"}, &result); err != nil {
		t.Fatal(err)
	}
	if result.Value != "ok" || provider.calls.Load() != 1 {
		t.Fatalf("output=%#v provider calls=%d", result, provider.calls.Load())
	}
}

func TestMapperRegressionRejectsDeclarationsBeforeProvider(t *testing.T) {
	type input struct{ Token string }
	type output struct{ Value int }
	for _, test := range []struct {
		name   string
		change func(*Config)
		want   string
	}{
		{"missing endpoint", func(c *Config) { c.Client.HTTP.URL = "" }, "url is required"},
		{"method", func(c *Config) { c.Client.HTTP.Method = "TRACE" }, "http method"},
		{"unknown input", func(c *Config) { c.Request = []RequestMapping{{Input: "Missing", Header: "X-Test"}} }, `input path "Missing"`},
		{"unknown output", func(c *Config) { c.Response.Mappings[0].Output = "Missing" }, `field "Missing" was not found`},
		{"duplicate output", func(c *Config) {
			c.Response.Mappings = append(c.Response.Mappings, ResponseMapping{Path: "/other", Output: "Value"})
		}, "duplicate binding path"},
		{"bad response pointer", func(c *Config) { c.Response.Mappings[0].Path = "value" }, "JSON pointer"},
		{"unmapped placeholder", func(c *Config) { c.Client.HTTP.URL += "/{id}" }, "has no path request mapping"},
		{"wrong transport", func(c *Config) {
			c.Client = ClientOptions{Transport: TransportMCP, MCP: &xmcp.Options{URL: "https://example.test/mcp", Tool: "lookup"}}
		}, "selects transport"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := regressionHTTPConfig()
			test.change(&config)
			provider := &regressionHTTPProvider{}
			err := NewMapper().HTTP(context.Background(), &config, provider, nil, &input{}, &output{})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v, want %q", err, test.want)
			}
			if provider.calls.Load() != 0 {
				t.Fatalf("provider called %d times", provider.calls.Load())
			}
		})
	}
	config := regressionHTTPConfig()
	for _, test := range []struct {
		name, want string
		in, out    any
	}{
		{"nil input", "remote input", (*input)(nil), &output{}},
		{"nonstruct input", "remote input", new(int), &output{}},
		{"nil output", "remote output", &input{}, (*output)(nil)},
		{"nonstruct output", "remote output", &input{}, new(int)},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &regressionHTTPProvider{}
			err := NewMapper().HTTP(context.Background(), &config, provider, nil, test.in, test.out)
			if err == nil || !strings.Contains(err.Error(), test.want) || provider.calls.Load() != 0 {
				t.Fatalf("error=%v provider calls=%d", err, provider.calls.Load())
			}
		})
	}
}

func TestMapperRegressionRequiresNamedCacheProvider(t *testing.T) {
	config := regressionHTTPConfig()
	config.Cache = &Cache{TTL: "1m"}
	httpProvider := &regressionHTTPProvider{}
	var output struct{ Value int }
	err := NewMapper().HTTP(context.Background(), &config, httpProvider, nil, &struct{}{}, &output)
	if err == nil || !strings.Contains(err.Error(), "cache name is required") || httpProvider.calls.Load() != 0 {
		t.Fatalf("missing cache name error=%v provider calls=%d", err, httpProvider.calls.Load())
	}
	config.Cache.Name = "missing"
	cacheProvider := newTestCache(t, 0)
	err = NewMapper().HTTP(context.Background(), &config, httpProvider, cacheProvider, &struct{}{}, &output)
	if err == nil || !strings.Contains(err.Error(), `cache "missing" is not registered`) || httpProvider.calls.Load() != 0 {
		t.Fatalf("unregistered cache error=%v provider calls=%d", err, httpProvider.calls.Load())
	}
	config.Cache.Name = testCacheName
	err = NewMapper().HTTP(context.Background(), &config, httpProvider, nil, &struct{}{}, &output)
	if err == nil || !strings.Contains(err.Error(), "cache provider is not bound") || httpProvider.calls.Load() != 0 {
		t.Fatalf("unbound cache error=%v provider calls=%d", err, httpProvider.calls.Load())
	}
}

func TestMapperRegressionMissingPathValueBeforeProvider(t *testing.T) {
	type input struct{ ID *string }
	type output struct{ Value int }
	config := regressionHTTPConfig()
	config.Client.HTTP.URL += "/{id}"
	config.Request = []RequestMapping{{Input: "ID", Path: "id"}}
	provider := &regressionHTTPProvider{client: &regressionHTTPClient{do: func(*http.Request) (*http.Response, error) {
		t.Fatal("network called for missing path value")
		return nil, nil
	}}}
	err := NewMapper().HTTP(context.Background(), &config, provider, nil, &input{}, &output{})
	if err == nil || !strings.Contains(err.Error(), "has no value") {
		t.Fatalf("missing path error: %v", err)
	}
	if provider.calls.Load() != 0 {
		t.Fatalf("provider called %d times before path validation", provider.calls.Load())
	}
}

func TestMapperRegressionNestedPointerAndOptionalOutput(t *testing.T) {
	type profile struct{ Name string }
	type contextOutput struct{ Profile *profile }
	type output struct{ Context *contextOutput }
	config := regressionHTTPConfig()
	config.Response.Mappings = []ResponseMapping{{Path: "/outer/items/0/~1name", Output: "Context.Profile.Name"}}
	body := `{"outer":{"items":[{"/name":"Ada"}]}}`
	provider := &regressionHTTPProvider{client: &regressionHTTPClient{do: func(*http.Request) (*http.Response, error) {
		return regressionHTTPResponse(http.StatusOK, body), nil
	}}}
	mapper := NewMapper()
	var result output
	if err := mapper.HTTP(context.Background(), &config, provider, nil, &struct{}{}, &result); err != nil {
		t.Fatal(err)
	}
	if result.Context == nil || result.Context.Profile == nil || result.Context.Profile.Name != "Ada" {
		t.Fatalf("nested output not allocated: %#v", result)
	}
	config.Response.Mappings[0].Optional = true
	body = `{}`
	result = output{}
	if err := mapper.HTTP(context.Background(), &config, provider, nil, &struct{}{}, &result); err != nil {
		t.Fatal(err)
	}
	if result.Context != nil {
		t.Fatalf("optional missing mapping allocated output: %#v", result)
	}
	config.Response.Mappings[0].Optional = false
	if err := mapper.HTTP(context.Background(), &config, provider, nil, &struct{}{}, &result); err == nil || !strings.Contains(err.Error(), "is missing") {
		t.Fatalf("required missing mapping error: %v", err)
	}
}

func TestMapperRegressionMalformedOutputIsNotCached(t *testing.T) {
	config := regressionHTTPConfig()
	config.Cache = &Cache{Name: testCacheName, TTL: "1m"}
	cacheProvider := newTestCache(t, 0)
	var calls atomic.Int64
	provider := &regressionHTTPProvider{client: &regressionHTTPClient{do: func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return regressionHTTPResponse(http.StatusOK, `{"value":"not an integer"}`), nil
		}
		return regressionHTTPResponse(http.StatusOK, `{"value":7}`), nil
	}}}
	mapper := NewMapper()
	var result struct{ Value int }
	if err := mapper.HTTP(context.Background(), &config, provider, cacheProvider, &struct{}{}, &result); err == nil || !strings.Contains(err.Error(), "cannot populate") {
		t.Fatalf("malformed output error: %v", err)
	}
	if entries := testEntries(t, mapper, &config, cacheProvider); entries != 0 {
		t.Fatalf("malformed output cached: %d entries", entries)
	}
	for i := 0; i < 2; i++ {
		result = struct{ Value int }{}
		if err := mapper.HTTP(context.Background(), &config, provider, cacheProvider, &struct{}{}, &result); err != nil || result.Value != 7 {
			t.Fatalf("attempt %d: output=%#v err=%v", i, result, err)
		}
	}
	if entries := testEntries(t, mapper, &config, cacheProvider); calls.Load() != 2 || entries != 1 {
		t.Fatalf("remote calls=%d cache entries=%d", calls.Load(), entries)
	}
}

func TestMapperRegressionHTTPFailuresAreNotCached(t *testing.T) {
	config := regressionHTTPConfig()
	config.Cache = &Cache{Name: testCacheName, TTL: "1m"}
	cacheProvider := newTestCache(t, 0)
	var calls atomic.Int64
	provider := &regressionHTTPProvider{client: &regressionHTTPClient{do: func(*http.Request) (*http.Response, error) {
		switch calls.Add(1) {
		case 1:
			return regressionHTTPResponse(http.StatusForbidden, `{"value":8}`), nil
		case 2:
			return regressionHTTPResponse(http.StatusOK, `{"value":`), nil
		default:
			return regressionHTTPResponse(http.StatusOK, `{"value":8}`), nil
		}
	}}}
	mapper := NewMapper()
	var output struct{ Value int }
	err := mapper.HTTP(context.Background(), &config, provider, cacheProvider, &struct{}{}, &output)
	var status *StatusError
	if entries := testEntries(t, mapper, &config, cacheProvider); !errors.As(err, &status) || status.StatusCode != http.StatusForbidden || entries != 0 {
		t.Fatalf("HTTP status error=%v cache entries=%d", err, entries)
	}
	err = mapper.HTTP(context.Background(), &config, provider, cacheProvider, &struct{}{}, &output)
	if entries := testEntries(t, mapper, &config, cacheProvider); err == nil || !strings.Contains(err.Error(), "decode JSON response") || entries != 0 {
		t.Fatalf("malformed JSON error=%v cache entries=%d", err, entries)
	}
	for i := 0; i < 2; i++ {
		output = struct{ Value int }{}
		if err := mapper.HTTP(context.Background(), &config, provider, cacheProvider, &struct{}{}, &output); err != nil || output.Value != 8 {
			t.Fatalf("attempt %d output=%#v err=%v", i, output, err)
		}
	}
	if entries := testEntries(t, mapper, &config, cacheProvider); calls.Load() != 3 || entries != 1 {
		t.Fatalf("HTTP calls=%d cache entries=%d", calls.Load(), entries)
	}
}

func TestMapperRegressionCacheTTLPartitionCapAndInvalidation(t *testing.T) {
	type input struct{ Tenant, Token string }
	type output struct{ Value int }
	var tick atomic.Int64
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return start.Add(time.Duration(tick.Load()) * time.Second) }
	config := regressionHTTPConfig()
	config.Request = []RequestMapping{{Input: "Token", Header: "Authorization"}}
	config.Cache = &Cache{Name: testCacheName, TTL: "10s", Partition: []string{"Tenant"}}
	cacheProvider := newTestCache(t, 2)
	var calls atomic.Int64
	// Each cache miss returns its invocation number, so hits are observable.
	provider := &regressionHTTPProvider{client: &regressionHTTPClient{do: func(*http.Request) (*http.Response, error) {
		value := calls.Add(1)
		return regressionHTTPResponse(http.StatusOK, `{"value":`+strconv.FormatInt(value, 10)+`}`), nil
	}}}
	mapper := NewMapper(WithClock(clock))
	get := func(tenant string) int {
		t.Helper()
		var result output
		if err := mapper.HTTP(context.Background(), &config, provider, cacheProvider, &input{Tenant: tenant, Token: "same-token"}, &result); err != nil {
			t.Fatal(err)
		}
		return result.Value
	}
	if value := get("a"); value != 1 {
		t.Fatalf("a first=%d", value)
	}
	if value := get("a"); value != 1 {
		t.Fatalf("a cached=%d", value)
	}
	tick.Store(1)
	if value := get("b"); value != 2 {
		t.Fatalf("b first=%d", value)
	}
	tick.Store(2)
	if value := get("c"); value != 3 || testEntries(t, mapper, &config, cacheProvider) != 2 {
		t.Fatalf("c first=%d entries=%d", value, testEntries(t, mapper, &config, cacheProvider))
	}
	if removed, err := mapper.Invalidate(context.Background(), &config, cacheProvider, "b"); err != nil || removed != 1 {
		t.Fatalf("invalidated b entries=%d err=%v", removed, err)
	}
	if value := get("c"); value != 3 {
		t.Fatalf("c after b invalidation=%d", value)
	}
	if value := get("a"); value != 4 {
		t.Fatalf("evicted a=%d", value)
	}
	tick.Store(13)
	if value := get("c"); value != 5 {
		t.Fatalf("expired c=%d", value)
	}
	if value := get("a"); value != 6 {
		t.Fatalf("expired a=%d", value)
	}
	if removed, err := mapper.Invalidate(context.Background(), &config, cacheProvider); err != nil || removed != 2 || testEntries(t, mapper, &config, cacheProvider) != 0 {
		t.Fatalf("clear removed=%d entries=%d err=%v", removed, testEntries(t, mapper, &config, cacheProvider), err)
	}
}

func TestMapperRegressionResponseValidityCapsTTL(t *testing.T) {
	var tick atomic.Int64
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	config := regressionHTTPConfig()
	config.Cache = &Cache{Name: testCacheName, TTL: "1m"}
	cacheProvider := newTestCache(t, 0)
	config.Response.Validity = &Validity{Path: "/validFor", Format: ValiditySeconds}
	var calls atomic.Int64
	provider := &regressionHTTPProvider{client: &regressionHTTPClient{do: func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return regressionHTTPResponse(http.StatusOK, `{"value":4,"validFor":2}`), nil
	}}}
	mapper := NewMapper(WithClock(func() time.Time { return start.Add(time.Duration(tick.Load()) * time.Second) }))
	get := func() {
		t.Helper()
		var output struct{ Value int }
		if err := mapper.HTTP(context.Background(), &config, provider, cacheProvider, &struct{}{}, &output); err != nil || output.Value != 4 {
			t.Fatalf("output=%#v err=%v", output, err)
		}
	}
	get()
	tick.Store(1)
	get()
	if calls.Load() != 1 {
		t.Fatalf("valid cache calls=%d", calls.Load())
	}
	tick.Store(2)
	get()
	if calls.Load() != 2 {
		t.Fatalf("validity expiry calls=%d", calls.Load())
	}
}

func TestMapperRegressionZeroBackendCapacityIsUnlimited(t *testing.T) {
	type input struct{ Key string }
	config := regressionHTTPConfig()
	config.Request = []RequestMapping{{Input: "Key", Header: "X-Key"}}
	config.Cache = &Cache{Name: testCacheName, TTL: "1m"}
	cacheProvider := newTestCache(t, 0)
	var calls atomic.Int64
	provider := &regressionHTTPProvider{client: &regressionHTTPClient{do: func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return regressionHTTPResponse(http.StatusOK, `{"value":1}`), nil
	}}}
	mapper := NewMapper()
	for _, key := range []string{"a", "b", "c", "a", "b", "c"} {
		var output struct{ Value int }
		if err := mapper.HTTP(context.Background(), &config, provider, cacheProvider, &input{Key: key}, &output); err != nil || output.Value != 1 {
			t.Fatalf("key=%q output=%#v err=%v", key, output, err)
		}
	}
	if entries := testEntries(t, mapper, &config, cacheProvider); calls.Load() != 3 || entries != 3 {
		t.Fatalf("unlimited cache calls=%d entries=%d", calls.Load(), entries)
	}
}

func TestMapperRegressionCoalescingAndWaiterCancellation(t *testing.T) {
	config := regressionHTTPConfig()
	config.Cache = &Cache{Name: testCacheName, TTL: "1m"}
	cacheProvider := newTestCache(t, 0)
	started := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var calls atomic.Int64
	provider := &regressionHTTPProvider{client: &regressionHTTPClient{do: func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return regressionHTTPResponse(http.StatusOK, `{"value":5}`), nil
	}}}
	mapper := NewMapper()
	invoke := func(ctx context.Context) error {
		var output struct{ Value int }
		err := mapper.HTTP(ctx, &config, provider, cacheProvider, &struct{}{}, &output)
		if err == nil && output.Value != 5 {
			return errors.New("unexpected coalesced output")
		}
		return err
	}
	leader := make(chan error, 1)
	go func() { leader <- invoke(context.Background()) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not reach client")
	}
	waiterCtx, cancel := context.WithCancel(context.Background())
	waiter := make(chan error, 1)
	entered := make(chan struct{}, 2)
	go func() { entered <- struct{}{}; waiter <- invoke(waiterCtx) }()
	shared := make(chan error, 1)
	go func() { entered <- struct{}{}; shared <- invoke(context.Background()) }()
	<-entered
	<-entered
	for i := 0; i < 100; i++ {
		runtime.Gosched()
	}
	cancel()
	select {
	case err := <-waiter:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter error=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled waiter remained blocked on leader")
	}
	if calls.Load() != 1 {
		t.Fatalf("coalesced client calls=%d", calls.Load())
	}
	close(release)
	if err := <-leader; err != nil {
		t.Fatal(err)
	}
	if err := <-shared; err != nil {
		t.Fatal(err)
	}
	if err := invoke(context.Background()); err != nil || calls.Load() != 1 {
		t.Fatalf("cached post-leader invocation err=%v client calls=%d", err, calls.Load())
	}
}

func TestMapperRegressionMCPStructuredTextAndErrors(t *testing.T) {
	type input struct{ Tenant string }
	type output struct{ Value string }
	config := regressionMCPConfig()
	config.Request = []RequestMapping{{Input: "Tenant", Argument: "tenant"}, {Input: "Tenant", Header: "X-Tenant"}}
	responses := []*schema.CallToolResult{
		{StructuredContent: map[string]any{"value": "structured"}},
		{Content: []schema.CallToolResultContentElem{schema.TextContent{Type: "text", Text: `{"value":"text"}`}}},
		{IsError: func() *bool { value := true; return &value }(), Content: []schema.CallToolResultContentElem{schema.TextContent{Type: "text", Text: "denied"}}},
	}
	var calls atomic.Int64
	provider := &regressionMCPProvider{client: &regressionMCPClient{call: func(_ context.Context, request *schema.CallToolRequest, options xmcp.CallOptions) (*schema.CallToolResult, error) {
		if request.Params.Name != "lookup" || options.Header.Get("X-Tenant") != "a" {
			t.Fatalf("MCP request=%#v headers=%#v", request.Params, options.Header)
		}
		if request.Params.Arguments["tenant"] != "a" {
			t.Fatalf("MCP arguments=%#v", request.Params.Arguments)
		}
		return responses[int(calls.Add(1))-1], nil
	}}}
	mapper := NewMapper()
	var result output
	if err := mapper.MCP(context.Background(), &config, provider, nil, &input{Tenant: "a"}, &result); err != nil || result.Value != "structured" {
		t.Fatalf("structured output=%#v err=%v", result, err)
	}
	config.Response.Source = SourceText
	result = output{}
	if err := mapper.MCP(context.Background(), &config, provider, nil, &input{Tenant: "a"}, &result); err != nil || result.Value != "text" {
		t.Fatalf("text output=%#v err=%v", result, err)
	}
	result = output{}
	err := mapper.MCP(context.Background(), &config, provider, nil, &input{Tenant: "a"}, &result)
	var toolErr *ToolError
	if !errors.As(err, &toolErr) || !strings.Contains(toolErr.Message, "denied") {
		t.Fatalf("tool error=%v", err)
	}
}

func TestMapperRegressionTypedNilProvidersAndClients(t *testing.T) {
	httpConfig, mcpConfig := regressionHTTPConfig(), regressionMCPConfig()
	var output struct{ Value int }
	var nilHTTPProvider *regressionHTTPProvider
	if err := NewMapper().HTTP(context.Background(), &httpConfig, nilHTTPProvider, nil, &struct{}{}, &output); err == nil || !strings.Contains(err.Error(), "provider is not bound") {
		t.Fatalf("typed nil HTTP provider error=%v", err)
	}
	var nilHTTPClient *regressionHTTPClient
	if err := NewMapper().HTTP(context.Background(), &httpConfig, &regressionHTTPProvider{client: nilHTTPClient}, nil, &struct{}{}, &output); err == nil || !strings.Contains(err.Error(), "returned no client") {
		t.Fatalf("typed nil HTTP client error=%v", err)
	}
	var nilMCPProvider *regressionMCPProvider
	if err := NewMapper().MCP(context.Background(), &mcpConfig, nilMCPProvider, nil, &struct{}{}, &output); err == nil || !strings.Contains(err.Error(), "provider is not bound") {
		t.Fatalf("typed nil MCP provider error=%v", err)
	}
	var nilMCPClient *regressionMCPClient
	if err := NewMapper().MCP(context.Background(), &mcpConfig, &regressionMCPProvider{client: nilMCPClient}, nil, &struct{}{}, &output); err == nil || !strings.Contains(err.Error(), "returned no client") {
		t.Fatalf("typed nil MCP client error=%v", err)
	}
}
