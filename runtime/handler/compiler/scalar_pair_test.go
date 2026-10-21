package compiler

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	bs "github.com/viant/bindly/state"
	mi "github.com/viant/datly/mcp/input"
	"github.com/viant/datly/mcp/tool"
	"github.com/viant/datly/spec"
)

type pairName string

func (p pairName) String() string { return string(p) }

type pairCount int

func (p pairCount) Count() int { return int(p) }

func TestExplicitScalarPairCompileDescribeAndRuntime(t *testing.T) {
	for _, tc := range []struct {
		name, wire, destination, schema string
		typ                             reflect.Type
		zero, value                     any
		nullable                        bool
	}{
		{"int", "*int", "int", "integer", reflect.TypeFor[int](), 0, 7, true},
		{"named string", "*fixture.Name", "fixture.Name", "string", reflect.TypeFor[pairName](), "", "seven", true},
		{"named int", "*fixture.Count", "fixture.Count", "integer", reflect.TypeFor[pairCount](), 0, 7, true},
		{"inverse bool", "bool", "*bool", "boolean", reflect.TypeFor[*bool](), false, true, false},
	} {
		for _, kind := range []string{"query", "form"} {
			for _, def := range []string{"", "7"} {
				if def != "" && tc.schema != "integer" {
					continue
				}
				t.Run(tc.name+"/"+kind+"/default="+def, func(t *testing.T) {
					inputType := reflect.StructOf([]reflect.StructField{{Name: "Value", Type: tc.typ, Tag: reflect.StructTag(fmt.Sprintf(`parameter:",kind=%s,in=value,dataType=%s"`, kind, tc.wire))}, {Name: "Has", Type: reflect.TypeFor[struct{ Value bool }](), Tag: `setMarker:"true" json:"-"`}})
					lookup := func(name string) (reflect.Type, error) {
						switch name {
						case "fixture.Name":
							return reflect.TypeFor[pairName](), nil
						case "fixture.Count":
							return reflect.TypeFor[pairCount](), nil
						}
						return nil, fmt.Errorf("unknown %s", name)
					}
					param := &spec.Parameter{Name: "Value", TypeExpr: tc.wire, OutputTypeExpr: tc.destination, Source: spec.BindSource{Kind: kind, Name: "value"}}
					if def != "" {
						v := def
						param.Value = &v
					}
					component := &spec.Component{Parameters: []*spec.Parameter{param}, Routes: []*spec.Route{{Method: "POST", Path: "/scalar"}}}
					compile := func(describe bool, p *spec.Parameter) *Result {
						c := *component
						c.Parameters = []*spec.Parameter{p}
						compiler := New(Input{Component: &c, InputType: inputType, TypeLookup: lookup})
						var r *Result
						var err error
						if describe {
							r, err = compiler.Describe()
						} else {
							r, err = compiler.Compile()
						}
						require.NoError(t, err)
						return r
					}
					result := compile(false, param)
					route, ok := result.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/scalar"})
					require.True(t, ok)
					described := compile(true, param)
					describedRoute, ok := described.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/scalar"})
					require.True(t, ok)
					field := route.Fields()[0]
					require.Equal(t, tc.typ, field.SourceType())
					require.Equal(t, tc.typ, field.DestinationType())
					require.Nil(t, field.Binding().Transformer)
					require.Equal(t, &spec.WireSchema{Type: tc.schema, Nullable: tc.nullable}, field.WireSchema())
					require.Equal(t, field.WireSchema(), describedRoute.Fields()[0].WireSchema())
					schema := field.WireSchema()
					schema.Nullable = !schema.Nullable
					require.Equal(t, tc.nullable, field.WireSchema().Nullable)
					require.Nil(t, param.WireSchema)
					_, err := tool.NewCompiler().Compile(tool.Input{Contract: route, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "scalar"}})
					require.NoError(t, err)
					control := *param
					control.TypeExpr = tc.destination
					control.OutputTypeExpr = ""
					baseline := compile(false, &control)
					baselineRoute, _ := baseline.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/scalar"})
					for _, test := range []struct {
						name string
						args mi.Arguments
					}{{"missing", mi.Arguments{}}, {"null", mi.Arguments{"Value": nil}}, {"zero", mi.Arguments{"Value": tc.zero}}, {"value", mi.Arguments{"Value": tc.value}}} {
						t.Run("mcp/"+test.name, func(t *testing.T) {
							adapter, err := mi.NewCompiler().Compile([]mi.Argument{{PublicName: "Value", Source: bs.Location{Kind: kind, In: "value"}, SourceType: tc.typ}})
							require.NoError(t, err)
							scope, err := adapter.Scope(test.args)
							require.NoError(t, err)
							injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
							require.NoError(t, err)
							got, want := reflect.New(inputType), reflect.New(inputType)
							gotErr := injector.Bind(context.Background(), got.Interface(), bindly.WithPlan(route.Plan()))
							wantErr := injector.Bind(context.Background(), want.Interface(), bindly.WithPlan(baselineRoute.Plan()))
							require.Equal(t, fmt.Sprint(wantErr), fmt.Sprint(gotErr))
							require.Equal(t, want.Interface(), got.Interface())
							if tc.name == "inverse bool" {
								v := got.Elem().FieldByName("Value")
								if test.name == "missing" || test.name == "null" {
									require.True(t, v.IsNil())
								} else {
									require.False(t, v.IsNil())
									require.Equal(t, test.name == "value", v.Elem().Bool())
									require.True(t, got.Elem().FieldByName("Has").FieldByName("Value").Bool())
								}
							}
						})
					}
					for _, test := range []struct {
						name   string
						values url.Values
					}{{"missing", url.Values{}}, {"empty", url.Values{"value": {""}}}, {"zero", url.Values{"value": {fmt.Sprint(tc.zero)}}}, {"value", url.Values{"value": {fmt.Sprint(tc.value)}}}} {
						t.Run("http/"+test.name, func(t *testing.T) {
							providers := requestprovider.NewValues(requestprovider.WithQuery(test.values))
							if kind == "form" {
								providers = requestprovider.NewValues(requestprovider.WithForm(test.values))
							}
							injector, err := bindly.NewInjector(bindly.WithProviders(providers.Providers()...))
							require.NoError(t, err)
							got, want := reflect.New(inputType), reflect.New(inputType)
							gotErr := injector.Bind(context.Background(), got.Interface(), bindly.WithPlan(route.Plan()))
							wantErr := injector.Bind(context.Background(), want.Interface(), bindly.WithPlan(baselineRoute.Plan()))
							require.Equal(t, fmt.Sprint(wantErr), fmt.Sprint(gotErr))
							require.Equal(t, want.Interface(), got.Interface())
						})
					}
				})
			}
		}
	}
}

func TestScalarPairPathPreservesRequiredPolicy(t *testing.T) {
	for _, required := range []*bool{nil, new(bool), func() *bool { v := true; return &v }()} {
		for _, path := range []string{"/scalar/{value}", "/scalar"} {
			t.Run(fmt.Sprintf("%s/%v", path, required), func(t *testing.T) {
				typ := reflect.StructOf([]reflect.StructField{{Name: "Value", Type: reflect.TypeFor[int](), Tag: `parameter:",kind=path,in=value,dataType=*int"`}})
				build := func(wire string) (*Result, *tool.Plan) {
					p := &spec.Parameter{Name: "Value", TypeExpr: wire, Source: spec.BindSource{Kind: "path", Name: "value"}, Required: required}
					c, err := New(Input{Component: &spec.Component{Parameters: []*spec.Parameter{p}, Routes: []*spec.Route{{Method: "GET", Path: path}}}, InputType: typ}).Compile()
					require.NoError(t, err)
					route, ok := c.Input.ForRoute(spec.RouteRef{Method: "GET", Path: path})
					require.True(t, ok)
					plan, err := tool.NewCompiler().Compile(tool.Input{Contract: route, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "path"}})
					require.NoError(t, err)
					return c, plan
				}
				actual, plan := build("*int")
				baseline, control := build("int")
				require.Equal(t, control.Metadata().InputSchema.Required, plan.Metadata().InputSchema.Required)
				require.Equal(t, baseline.Bindings[0].Required, actual.Bindings[0].Required)
				require.Equal(t, baseline.Bindings[0].URI, actual.Bindings[0].URI)
				route, _ := actual.Input.ForRoute(spec.RouteRef{Method: "GET", Path: path})
				require.Equal(t, reflect.TypeFor[int](), route.Fields()[0].DestinationType())
				require.Equal(t, &spec.WireSchema{Type: "integer", Nullable: true}, route.Fields()[0].WireSchema())
			})
		}
	}
}

func TestScalarPairNeedsExplicitAlternateAuthority(t *testing.T) {
	for _, tc := range []struct {
		data string
		typ  reflect.Type
	}{{"", reflect.TypeFor[*bool]()}, {"*bool", reflect.TypeFor[*bool]()}, {"bool", reflect.TypeFor[bool]()}, {"*int32", reflect.TypeFor[int64]()}, {"**int", reflect.TypeFor[*int]()}, {"*float64", reflect.TypeFor[float64]()}, {"*[]int", reflect.TypeFor[[]int]()}, {"*map[string]int", reflect.TypeFor[map[string]int]()}} {
		binding := bindly.BindingSpec{DataType: tc.data, Location: bs.Location{Kind: "query"}}
		require.NoError(t, applyExplicitScalarWire(reflect.StructField{Type: tc.typ}, &binding))
		require.Nil(t, binding.Extension)
	}
}
