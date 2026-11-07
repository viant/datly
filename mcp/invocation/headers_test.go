package invocation

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/mcp-protocol/authorization"
)

func TestRequestHeadersAreIsolatedAndUseVerifiedAuthorization(t *testing.T) {
	headers := http.Header{"X-Tag": {"one", "two"}, "authorization": {"raw-unverified"}, "Mcp-Session-Id": {"session-a"}}
	ctx := WithHTTPHeaders(context.Background(), headers)
	headers["X-Tag"][0] = "mutated"
	first := RequestHeaders(ctx)
	if !reflect.DeepEqual(first.Values("X-Tag"), []string{"one", "two"}) || first.Get("Authorization") != "" || first.Get("Mcp-Session-Id") != "session-a" {
		t.Fatalf("unexpected headers: %v", first)
	}
	first.Set("X-Tag", "changed")
	if RequestHeaders(ctx).Get("X-Tag") != "one" {
		t.Fatal("returned map mutated context")
	}
	ctx = context.WithValue(ctx, authorization.TokenKey, &authorization.Token{Token: "verified-fixture"})
	if RequestHeaders(ctx).Get("Authorization") != "Bearer verified-fixture" {
		t.Fatal("verified credential not projected")
	}
	if len(RequestHeaders(context.Background())) != 0 {
		t.Fatal("headers leaked into another request")
	}
	if RequestHeaders(WithHTTPHeaders(ctx, nil)).Get("Authorization") != "Bearer verified-fixture" {
		t.Fatal("empty transport headers lost verified credential")
	}
}

func TestRequestProvidersPreserveTransportHeadersAndArgumentFallback(t *testing.T) {
	ctx := WithHTTPHeaders(t.Context(), http.Header{"X-Tag": {"one", "two"}, "X-Mode": {"transport"}})
	ctx = context.WithValue(ctx, authorization.TokenKey, &authorization.Token{Token: "verified-fixture"})
	scope := requestprovider.NewValues(requestprovider.WithHeaders(http.Header{"X-Mode": {"argument"}, "X-Other": {"fallback"}, "Authorization": {"argument-credential"}}))
	providers, err := requestProviders(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	root, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	binder, err := root.ForScope(providers...)
	if err != nil {
		t.Fatal(err)
	}
	var input struct {
		Tags          []string `parameter:",kind=header,in=X-Tag"`
		Mode          string   `parameter:",kind=header,in=X-Mode"`
		Other         string   `parameter:",kind=header,in=X-Other"`
		Authorization string   `parameter:",kind=header,in=Authorization"`
	}
	if err = binder.Bind(ctx, &input); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(input.Tags, []string{"one", "two"}) || input.Mode != "transport" || input.Other != "fallback" || input.Authorization != "Bearer verified-fixture" {
		t.Fatalf("incorrect header binding: %+v", input)
	}
	original, _, err := scope.Header().Locate(nil).Value(ctx, reflect.TypeFor[string](), "X-Mode")
	if err != nil || original != "argument" {
		t.Fatalf("argument scope changed: %v %v", original, err)
	}
}
