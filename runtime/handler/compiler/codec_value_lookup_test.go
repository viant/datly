package compiler

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	xcodec "github.com/viant/xdatly/codec"
)

type invocationLookupCodec struct{}

func (invocationLookupCodec) Value(ctx context.Context, raw any, opts ...xcodec.Option) (any, error) {
	lookup := xcodec.NewOptions(opts).LookupValue
	if lookup == nil {
		return nil, fmt.Errorf("invocation value lookup unavailable")
	}
	suffix, err := lookup(ctx, "Delimiter")
	if err != nil {
		return nil, err
	}
	return raw.(string) + suffix.(string), nil
}

type optionalLookupCodec struct{}

type optionalLookupFactory struct{}

func (optionalLookupFactory) New(*xcodec.Config, ...xcodec.Option) (xcodec.Instance, error) {
	return optionalLookupCodec{}, nil
}

func TestOptionalCodecDependencyPreservesPresence(t *testing.T) {
	type presence struct{ Value, Delimiter bool }
	type input struct {
		Delimiter string
		Value     string
		Has       *presence `setMarker:"true" json:"-"`
	}
	optional := false
	component := &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/items"}}, Parameters: []*spec.Parameter{
		{Name: "Value", Source: spec.BindSource{Kind: "query", Name: "value"}, TypeExpr: "string", Codec: &spec.Codec{Body: "optionalLookup"}},
		{Name: "Delimiter", Source: spec.BindSource{Kind: "query", Name: "delimiter"}, TypeExpr: "string", Required: &optional},
	}}
	compiled, err := New(Input{Component: component, InputType: reflect.TypeFor[input](), CodecFactory: optionalLookupFactory{}}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/items"})
	if !ok {
		t.Fatal("input route missing")
	}
	for _, tc := range []struct {
		name    string
		query   url.Values
		present bool
		want    string
	}{
		// This direct source-state provider returns the field's zero value;
		// its lookup must not change the transport suppliedness marker.
		{"omitted", url.Values{"value": {"url"}}, false, "url"},
		{"explicit_empty", url.Values{"value": {"url"}, "delimiter": {""}}, true, "url"},
		{"explicit_value", url.Values{"value": {"url"}, "delimiter": {"|"}}, true, "url|"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			injector, err := bindly.NewInjector(bindly.WithProviders(provider.Parameter()))
			if err != nil {
				t.Fatal(err)
			}
			scope, err := injector.ForScope(testharness.Request{}.WithQuery(tc.query).Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			actual := &input{}
			if err := scope.Bind(t.Context(), actual, bindly.WithPlan(route.Plan()), bindly.WithSource(actual)); err != nil {
				t.Fatal(err)
			}
			if actual.Value != tc.want || actual.Has == nil || actual.Has.Delimiter != tc.present || !actual.Has.Value {
				t.Fatalf("presence/output = %+v", actual)
			}
		})
	}
}

func (optionalLookupCodec) Value(ctx context.Context, raw any, opts ...xcodec.Option) (any, error) {
	value, err := xcodec.NewOptions(opts).LookupValue(ctx, "Delimiter")
	if err != nil {
		return nil, err
	}
	if value == nil {
		return raw.(string) + "default", nil
	}
	return raw.(string) + value.(string), nil
}

func TestBindingCompilationMarksOptionalCodecDependencies(t *testing.T) {
	type input struct {
		Value     string `parameter:"Value,kind=query,in=value"`
		Delimiter string `parameter:"Delimiter,kind=query,in=delimiter,required=false"`
	}
	component := &spec.Component{Parameters: []*spec.Parameter{
		{Name: "Value", Source: spec.BindSource{Kind: "query", Name: "value"}},
	}}
	bindings, err := BuildBindingSpecs(component, reflect.TypeFor[input](), map[string]ParamCodec{"Value": {Instance: optionalLookupCodec{}, SourceType: reflect.TypeFor[string]()}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, binding := range bindings {
		if binding.Name != "Value" {
			continue
		}
		got, err := binding.Transformer.Transform(ctx, valueResolver{wantContext: ctx}, "url")
		if err != nil || got != "urldefault" {
			t.Fatalf("optional lookup = %v, %v", got, err)
		}
		return
	}
	t.Fatal("codec binding missing")
}

type valueResolver struct {
	wantContext context.Context
	value       any
	found       bool
	err         error
}

func (r valueResolver) Value(ctx context.Context, where *state.Location) (any, bool, error) {
	if ctx != r.wantContext {
		return nil, false, fmt.Errorf("codec lookup lost invocation context")
	}
	if where.Kind != "param" || where.In != "Delimiter" {
		return nil, false, fmt.Errorf("wrong parameter location: %+v", where)
	}
	return r.value, r.found, r.err
}
func TestParameterCodecLookupUsesInvocationResolver(t *testing.T) {
	transform := newCodecTransformer(invocationLookupCodec{})
	for _, delimiter := range []string{"\t", ",", "|"} {
		t.Run(fmt.Sprintf("delimiter%q", delimiter), func(t *testing.T) {
			t.Parallel()
			ctx := context.WithValue(context.Background(), struct{}{}, delimiter)
			for i := 0; i < 100; i++ {
				got, err := transform.Transform(ctx, valueResolver{wantContext: ctx, value: delimiter, found: true}, "url")
				if err != nil || got != "url"+delimiter {
					t.Fatalf("codec invocation result %v,%v", got, err)
				}
			}
		})
	}
}
func TestParameterCodecLookupRetainsMissingAndResolverFailures(t *testing.T) {
	transform := newCodecTransformer(invocationLookupCodec{})
	ctx := context.Background()
	cause := errors.New("dependency resolution failed")
	if _, err := transform.Transform(ctx, valueResolver{wantContext: ctx, err: cause}, "url"); !errors.Is(err, cause) {
		t.Fatalf("resolver failure lost: %v", err)
	}
	for _, resolver := range []valueResolver{{wantContext: ctx}, {wantContext: ctx, value: ",", found: false}} {
		if _, err := transform.Transform(ctx, resolver, "url"); err == nil || !strings.Contains(err.Error(), "Delimiter") {
			t.Fatalf("missing parameter was not reported: %v", err)
		}
	}
	if _, err := transform.Transform(ctx, nil, "url"); err == nil {
		t.Fatal("nil resolver lookup accepted")
	}
}
