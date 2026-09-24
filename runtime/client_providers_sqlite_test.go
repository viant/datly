package runtime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/provider/clients"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xhttp "github.com/viant/xdatly/client/http"
	xmcp "github.com/viant/xdatly/client/mcp"
)

// genericInput is an ordinary custom handler input: it consumes the public
// provider contract through native DI without importing the remote handler
// or any Datly client implementation.
type genericInput struct {
	Name string         `parameter:"Name,kind=query,in=name,required"`
	HTTP xhttp.Provider `bind:"kind=http_client,required"`
}

type genericOutput struct {
	Status int
	Body   string
}

// genericHandler resolves the configured client from the injected provider
// and executes one explicit request.
type genericHandler struct{ url string }

func (h *genericHandler) InputType() reflect.Type  { return reflect.TypeFor[genericInput]() }
func (h *genericHandler) OutputType() reflect.Type { return reflect.TypeFor[genericOutput]() }

func (h *genericHandler) Execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
	input := invocation.Input.(*genericInput)
	if input.HTTP == nil {
		return nil, fmt.Errorf("http provider was not injected")
	}
	client, err := input.HTTP.Client(ctx, xhttp.Options{URL: h.url, Method: http.MethodGet})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, h.url+"?name="+input.Name, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return &genericOutput{Status: response.StatusCode, Body: strings.TrimSpace(string(body))}, nil
}

type countingHTTPProvider struct {
	calls  atomic.Int64
	client xhttp.Client
}

func (p *countingHTTPProvider) Client(context.Context, xhttp.Options) (xhttp.Client, error) {
	p.calls.Add(1)
	return p.client, nil
}

type markingTransport struct{ mark string }

func (t markingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(t.mark)), Request: request}, nil
}

func genericComponent(t *testing.T, url string) *registry.RegisteredComponent {
	t.Helper()
	component := componentSpec("Generic", "GET", "/generic", []*spec.Parameter{{Name: "Name", Source: spec.BindSource{Kind: "query", Name: "name"}, Required: boolValue(true)}})
	artifact := componentArtifact(t, component, reflect.TypeFor[genericInput](), reflect.TypeFor[genericOutput]())
	return &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[genericOutput](), Handler: &genericHandler{url: url}}
}

func invokeGeneric(t *testing.T, rt *Runtime, name string) (*genericOutput, error) {
	t.Helper()
	request := testharness.NewRequest(http.MethodGet, "/generic").WithQuery(map[string][]string{"name": {name}})
	actual, err := executeTestRoute(t, rt, context.Background(), request)
	if err != nil {
		return nil, err
	}
	return actual.(*genericOutput), nil
}

func TestRuntimeSuppliesDefaultClientProvidersToOrdinaryComponents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "hello %s", r.URL.Query().Get("name"))
	}))
	defer server.Close()
	rt, err := NewRuntime([]*registry.RegisteredComponent{genericComponent(t, server.URL)})
	if err != nil {
		t.Fatal(err)
	}
	if kinds := providerKinds(rt.ClientProviders()); !reflect.DeepEqual(kinds, []string{clients.HTTPKind, clients.MCPKind}) {
		t.Fatalf("default provider kinds = %v", kinds)
	}
	output, err := invokeGeneric(t, rt, "alice")
	if err != nil || output.Status != http.StatusOK || output.Body != "hello alice" {
		t.Fatalf("output = %#v err = %v", output, err)
	}
	// Shutdown releases the runtime-owned default registry; borrowed
	// providers then fail closed instead of rebuilding clients.
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := invokeGeneric(t, rt, "alice"); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("shut down runtime served a client: %v", err)
	}
}

func TestComponentProvidersReplaceRuntimeDefaults(t *testing.T) {
	custom := &countingHTTPProvider{client: &http.Client{Transport: markingTransport{mark: "component-provider"}}}
	component := genericComponent(t, "http://never.dialed.test/ctx")
	component.Providers = clients.Providers(custom, nil)
	rt, err := NewRuntime([]*registry.RegisteredComponent{component})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	output, err := invokeGeneric(t, rt, "bob")
	if err != nil || output.Body != "component-provider" || custom.calls.Load() != 1 {
		t.Fatalf("output = %#v err = %v calls = %d", output, err, custom.calls.Load())
	}
	if kinds := providerKinds(component.Providers); !reflect.DeepEqual(kinds, []string{clients.HTTPKind}) {
		t.Fatalf("registration input was mutated: %v", kinds)
	}
}

func TestRuntimeOptionReplacesDefaultsAndFailsClosedForAbsentCapability(t *testing.T) {
	custom := &countingHTTPProvider{client: &http.Client{Transport: markingTransport{mark: "runtime-provider"}}}
	rt, err := NewRuntime([]*registry.RegisteredComponent{genericComponent(t, "http://never.dialed.test/ctx")}, WithClientProviders(custom, nil))
	if err != nil {
		t.Fatal(err)
	}
	if kinds := providerKinds(rt.ClientProviders()); !reflect.DeepEqual(kinds, []string{clients.HTTPKind}) {
		t.Fatalf("configured provider kinds = %v", kinds)
	}
	output, err := invokeGeneric(t, rt, "carol")
	if err != nil || output.Body != "runtime-provider" || custom.calls.Load() != 1 {
		t.Fatalf("output = %#v err = %v calls = %d", output, err, custom.calls.Load())
	}
	// Configured providers are borrowed: Shutdown must not disable them.
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if output, err := invokeGeneric(t, rt, "carol"); err != nil || output.Body != "runtime-provider" {
		t.Fatalf("borrowed provider was closed by the runtime: %#v %v", output, err)
	}
	// Supplying only an MCP provider leaves HTTP unavailable; the required
	// binding fails and no hidden default is constructed.
	mcpOnly, err := NewRuntime([]*registry.RegisteredComponent{genericComponent(t, "http://never.dialed.test/ctx")}, WithClientProviders(nil, mcpOnlyProvider{}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := invokeGeneric(t, mcpOnly, "dave"); err == nil {
		t.Fatal("missing http provider did not fail the invocation")
	}
	if _, err := NewRuntime(nil, WithClientProviders(nil, nil)); err == nil {
		t.Fatal("empty provider set accepted")
	}
}

type mcpOnlyProvider struct{}

func (mcpOnlyProvider) Client(context.Context, xmcp.Options) (xmcp.Client, error) {
	return nil, fmt.Errorf("not used")
}

func providerKinds(providers []locator.Provider) []string {
	var result []string
	for _, provider := range providers {
		result = append(result, provider.Kind())
	}
	return result
}
