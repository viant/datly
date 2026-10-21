package transcribe

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	bs "github.com/viant/bindly/state"
	mi "github.com/viant/datly/mcp/input"
	"github.com/viant/datly/mcp/tool"
	hc "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"net/url"
	"reflect"
	"testing"
)

type boolQueryInput struct {
	Flag bool                `parameter:",kind=query,in=flag,dataType=*bool"`
	Has  struct{ Flag bool } `setMarker:"true" json:"-"`
}

func TestExplicitBoolQueryWireRuntime(t *testing.T) {
	for _, defaultValue := range []string{"", "true", "false"} {
		t.Run("default="+defaultValue, func(t *testing.T) {
			param := &spec.Parameter{Name: "Flag", TypeExpr: "*bool", OutputTypeExpr: "bool", Source: spec.BindSource{Kind: "query", Name: "flag"}}
			if defaultValue != "" {
				v := defaultValue
				param.Value = &v
			}
			component := &spec.Component{Parameters: []*spec.Parameter{param}, Routes: []*spec.Route{{Method: "GET", Path: "/flag"}}}
			compiled, err := hc.New(hc.Input{Component: component, InputType: reflect.TypeFor[boolQueryInput]()}).Compile()
			require.NoError(t, err)
			route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/flag"})
			require.True(t, ok)
			baseline := *component
			baseline.Parameters = []*spec.Parameter{param.Clone()}
			baseline.Parameters[0].TypeExpr = "bool"
			control, err := hc.New(hc.Input{Component: &baseline, InputType: reflect.TypeFor[boolQueryInput]()}).Compile()
			require.NoError(t, err)
			controlRoute, ok := control.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/flag"})
			require.True(t, ok)
			plan, err := tool.NewCompiler().Compile(tool.Input{Contract: route, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "flag"}})
			require.NoError(t, err)
			require.Equal(t, []string{"boolean", "null"}, plan.Metadata().InputSchema.Properties["Flag"]["type"])
			originalPlan, err := tool.NewCompiler().Compile(tool.Input{Contract: controlRoute, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "flag"}})
			require.NoError(t, err)
			require.Equal(t, "boolean", originalPlan.Metadata().InputSchema.Properties["Flag"]["type"])
			require.Len(t, route.Fields(), 1)
			field := route.Fields()[0]
			require.Equal(t, reflect.TypeFor[bool](), field.SourceType())
			require.Equal(t, reflect.TypeFor[bool](), field.DestinationType())
			require.Nil(t, field.Binding().Transformer)
			require.Equal(t, controlRoute.Fields()[0].Binding().DefaultValue, field.Binding().DefaultValue)
			require.Equal(t, controlRoute.Fields()[0].Binding().Required, field.Binding().Required)
			for _, test := range []struct {
				name           string
				args           mi.Arguments
				present, value bool
			}{{"absent", mi.Arguments{}, false, false}, {"null", mi.Arguments{"Flag": nil}, false, false}, {"false", mi.Arguments{"Flag": false}, true, false}, {"true", mi.Arguments{"Flag": true}, true, true}} {
				t.Run("mcp/"+test.name, func(t *testing.T) {
					adapter, err := mi.NewCompiler().Compile([]mi.Argument{{PublicName: "Flag", Source: bs.Location{Kind: "query", In: "flag"}, SourceType: reflect.TypeFor[bool]()}})
					require.NoError(t, err)
					scope, err := adapter.Scope(test.args)
					require.NoError(t, err)
					injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
					require.NoError(t, err)
					var got, want boolQueryInput
					require.NoError(t, injector.Bind(t.Context(), &got, bindly.WithPlan(route.Plan())))
					require.NoError(t, injector.Bind(t.Context(), &want, bindly.WithPlan(controlRoute.Plan())))
					require.Equal(t, want, got)
					expected := test.value
					if !test.present && defaultValue != "" {
						expected = defaultValue == "true"
					}
					require.Equal(t, expected, got.Flag)
					if test.present {
						require.True(t, got.Has.Flag)
					} else if defaultValue == "" {
						require.False(t, got.Has.Flag)
					}
				})
			}
			for _, test := range []struct {
				name           string
				values         url.Values
				present, value bool
			}{{"absent", url.Values{}, false, false}, {"false", url.Values{"flag": {"false"}}, true, false}, {"true", url.Values{"flag": {"true"}}, true, true}} {
				t.Run("http/"+test.name, func(t *testing.T) {
					scope := requestprovider.NewValues(requestprovider.WithQuery(test.values))
					injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
					require.NoError(t, err)
					var got, want boolQueryInput
					require.NoError(t, injector.Bind(context.Background(), &got, bindly.WithPlan(route.Plan())))
					require.NoError(t, injector.Bind(t.Context(), &want, bindly.WithPlan(controlRoute.Plan())))
					require.Equal(t, want, got)
					expected := test.value
					if !test.present && defaultValue != "" {
						expected = defaultValue == "true"
					}
					require.Equal(t, expected, got.Flag)
					if test.present {
						require.True(t, got.Has.Flag)
					} else if defaultValue == "" {
						require.False(t, got.Has.Flag)
					}
				})
			}
			schemaCopy := route.Fields()[0].WireSchema()
			schemaCopy.Nullable = false
			require.True(t, route.Fields()[0].WireSchema().Nullable, "wire schema getter must return independent copies")
			require.Nil(t, param.WireSchema, "lowering must not mutate caller metadata")
		})
	}
}
func TestLinkedExplicitBoolQueryWire(t *testing.T) {
	resolver, err := typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.TranscribeAuthority, nil)
	require.NoError(t, err)
	for _, persisted := range []string{"bool", "*bool"} {
		p := &spec.Parameter{Name: "Flag", TypeExpr: "*bool", OutputTypeExpr: "bool", Source: spec.BindSource{Kind: "query", Name: "flag"}}
		actual := &spec.Parameter{Name: "Flag", TypeExpr: persisted, Source: p.Source}
		err := validateHandlerParameters(&HandlerBinding{input: reflect.TypeFor[boolQueryInput]()}, []*spec.Parameter{p}, []*spec.Parameter{actual}, resolver)
		if persisted == "*bool" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "type conflicts")
		}
	}
}

func TestLinkedBoolPointerDestinationUnchanged(t *testing.T) {
	type pointerInput struct {
		Flag *bool `parameter:",kind=query,in=flag,dataType=*bool"`
	}
	resolver, err := typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.TranscribeAuthority, nil)
	require.NoError(t, err)
	for _, output := range []string{"", "bool"} {
		p := &spec.Parameter{Name: "Flag", TypeExpr: "*bool", OutputTypeExpr: output, Source: spec.BindSource{Kind: "query", Name: "flag"}}
		actual := &spec.Parameter{Name: "Flag", TypeExpr: "*bool", Source: p.Source}
		err := validateHandlerParameters(&HandlerBinding{input: reflect.TypeFor[pointerInput]()}, []*spec.Parameter{p}, []*spec.Parameter{actual}, resolver)
		if output == "" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "unsupported contract overrides")
		}
	}
}
