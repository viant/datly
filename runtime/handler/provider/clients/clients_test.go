package clients

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	xhttp "github.com/viant/xdatly/client/http"
	xmcp "github.com/viant/xdatly/client/mcp"
)

type fakeHTTP struct{}

func (fakeHTTP) Client(context.Context, xhttp.Options) (xhttp.Client, error) {
	return &http.Client{}, nil
}

func kinds(providers []locator.Provider) []string {
	var result []string
	for _, provider := range providers {
		result = append(result, provider.Kind())
	}
	return result
}

func TestProvidersRegisterOnlySuppliedCapabilities(t *testing.T) {
	if got := Providers(nil, nil); len(got) != 0 {
		t.Fatalf("nil implementations registered %v", kinds(got))
	}
	var nilRegistry *Registry
	if got := nilRegistry.Providers(); len(got) != 0 || nilRegistry.HTTP() != nil || nilRegistry.MCP() != nil || nilRegistry.Close() != nil {
		t.Fatalf("nil registry constructed providers: %v", kinds(got))
	}
	httpOnly := Providers(fakeHTTP{}, nil)
	if !reflect.DeepEqual(kinds(httpOnly), []string{HTTPKind}) {
		t.Fatalf("kinds = %v", kinds(httpOnly))
	}
	owned := New()
	defer owned.Close()
	both := owned.Providers()
	if !reflect.DeepEqual(kinds(both), []string{HTTPKind, MCPKind}) {
		t.Fatalf("kinds = %v", kinds(both))
	}
}

func TestStaticProviderResolvesInterfaceValueFailClosed(t *testing.T) {
	owned := New()
	defer owned.Close()
	provider := Providers(owned.HTTP(), owned.MCP())[0]
	if provider.Priority() != locator.PriorityDefault {
		t.Fatalf("priority = %d", provider.Priority())
	}
	if cacheable, ok := provider.(locator.CachePolicy); !ok || !cacheable.DefaultCacheable() {
		t.Fatal("static interface value must be cacheable")
	}
	resolved, present, err := provider.Locate(nil).Value(context.Background(), reflect.TypeFor[xhttp.Provider](), "")
	if err != nil || !present {
		t.Fatalf("present=%v err=%v", present, err)
	}
	if _, ok := resolved.(xhttp.Provider); !ok {
		t.Fatalf("resolved %T is not the public provider contract", resolved)
	}
	if _, present, err := provider.Locate(nil).Value(context.Background(), reflect.TypeFor[xhttp.Provider](), "named"); err != nil || present {
		t.Fatalf("named lookup present=%v err=%v", present, err)
	}
	if _, _, err := provider.Locate(nil).Value(context.Background(), reflect.TypeFor[xmcp.Provider](), ""); err == nil {
		t.Fatal("http provider bound into an mcp target")
	}
	if _, _, err := provider.Locate(nil).Value(context.Background(), reflect.TypeFor[string](), ""); err == nil {
		t.Fatal("http provider bound into a string target")
	}
}

func TestRegistryOwnsLifecycle(t *testing.T) {
	owned := New()
	client, err := owned.HTTP().Client(context.Background(), xhttp.Options{URL: "http://127.0.0.1:9/ctx", Method: http.MethodGet})
	if err != nil || client == nil {
		t.Fatalf("client=%v err=%v", client, err)
	}
	if owned.Clients() != 1 {
		t.Fatalf("clients = %d", owned.Clients())
	}
	if err := owned.Close(); err != nil {
		t.Fatal(err)
	}
	if owned.Clients() != 0 {
		t.Fatalf("close retained %d clients", owned.Clients())
	}
	if _, err := owned.HTTP().Client(context.Background(), xhttp.Options{URL: "http://127.0.0.1:9/ctx", Method: http.MethodGet}); err == nil {
		t.Fatal("closed registry resolved a client")
	}
}
