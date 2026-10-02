package compiler

import (
	"context"
	"errors"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	bindstate "github.com/viant/bindly/state"
	mcpinput "github.com/viant/datly/mcp/input"
	"github.com/viant/datly/spec"
	xcodec "github.com/viant/xdatly/codec"
	xexec "github.com/viant/xdatly/exec"
	"net/url"
	"reflect"
	"testing"
)

func TestDefaultQueryCSVAndRepeatedLists(t *testing.T) {
	type input struct {
		Labels []string `parameter:",kind=query,in=labels"`
		IDs    []int    `parameter:",kind=query,in=id"`
		Plain  string   `parameter:",kind=query,in=plain"`
	}
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/lists"}}}, InputType: reflect.TypeFor[input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/lists"})
	if !ok {
		t.Fatal("route")
	}
	for _, tc := range []struct {
		name    string
		values  []string
		want    []int
		invalid bool
	}{{"CSV", []string{"1001,1002"}, []int{1001, 1002}, false}, {"repeated", []string{"1001", "1002"}, []int{1001, 1002}, false}, {"mixed keeps occurrence order", []string{"3,1", "2", "1,4"}, []int{3, 1, 2, 1, 4}, false}, {"absent", nil, nil, false}, {"empty", []string{""}, []int{}, false}, {"empty token", []string{"1,,2"}, []int{1, 2}, false}, {"invalid token", []string{"1,no,3"}, nil, true}} {
		t.Run(tc.name, func(t *testing.T) {
			values := url.Values{"plain": {"a,b"}, "labels": {"a,b", "c"}}
			if tc.values != nil {
				values["id"] = tc.values
			}
			providers := requestprovider.NewValues(requestprovider.WithQuery(values))
			injector, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			scope, err := injector.ForScope(providers.Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			var out input
			err = scope.Bind(context.Background(), &out, bindly.WithPlan(route.Plan()))
			if tc.invalid {
				var binding *bindly.BindingError
				if !errors.As(err, &binding) || binding.StatusCode() != 400 {
					t.Fatalf("error%v", err)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(out.IDs, tc.want) || out.Plain != "a,b" || !reflect.DeepEqual(out.Labels, []string{"a", "b", "c"}) {
				t.Fatalf("out%+v err%v", out, err)
			}
		})
	}
}
func TestQueryCSVTypedArrayAndMetadataRestrictions(t *testing.T) {
	transform := &queryListTransformer{target: reflect.TypeFor[[]int]()}
	input := []int{2, 1, 2}
	out, err := transform.Transform(context.Background(), nil, input)
	if err != nil || !reflect.DeepEqual(out, input) {
		t.Fatalf("typed array%v err%v", out, err)
	}
	for _, tc := range []struct {
		field reflect.StructField
		kind  string
	}{{reflect.StructField{Name: "IDs", Type: reflect.TypeFor[[]int](), Tag: `queryList:"bad"`}, "query"}, {reflect.StructField{Name: "ID", Type: reflect.TypeFor[string](), Tag: `queryList:"csv"`}, "query"}, {reflect.StructField{Name: "IDs", Type: reflect.TypeFor[[]int](), Tag: `queryList:"csv"`}, "body"}} {
		binding := bindly.BindingSpec{}
		binding.Location.Kind = tc.kind
		if err := applyQueryList(tc.field, false, &binding); err == nil {
			t.Fatal("invalid list metadata accepted")
		}
	}
}

func TestDefaultQueryPrimitiveScalar(t *testing.T) {
	for _, raw := range []any{1001, float64(1001)} {
		out, err := (&queryListTransformer{target: reflect.TypeFor[[]int]()}).Transform(context.Background(), nil, raw)
		if err != nil || !reflect.DeepEqual(out, []int{1001}) {
			t.Fatalf("scalar %T: %v, %v", raw, out, err)
		}
	}
}
func TestDefaultQueryListPreservesComplexShapesAndCodecs(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[[]struct{ ID int }](), reflect.TypeFor[[]*int](), reflect.TypeFor[[]map[string]int](), reflect.TypeFor[string]()} {
		binding := bindly.BindingSpec{}
		binding.Location.Kind = "query"
		if err := applyQueryList(reflect.StructField{Name: "Value", Type: typ}, false, &binding); err != nil || binding.Transformer != nil {
			t.Fatalf("%v modified: %v", typ, err)
		}
	}
}

func TestExplicitOptionalQueryListEmptyValue(t *testing.T) {
	optional, required := false, true
	for _, item := range []struct {
		name     string
		required *bool
		raw      []string
		invalid  bool
		want     []int
	}{
		{"optional empty", &optional, []string{""}, false, []int{}},
		{"unspecified empty", nil, []string{""}, false, []int{}},
		{"required empty transform", &required, []string{""}, false, []int{}},
		{"optional CSV hole", &optional, []string{"1,,2"}, false, []int{1, 2}},
		{"optional mixed empty", &optional, []string{"", "1"}, true, nil},
		{"optional repeated empty", &optional, []string{"", ""}, true, nil},
		{"optional whitespace", &optional, []string{" "}, false, []int{}},
	} {
		t.Run(item.name, func(t *testing.T) {
			binding := bindly.BindingSpec{Required: item.required}
			binding.Location.Kind = "query"
			if err := applyQueryList(reflect.StructField{Name: "IDs", Type: reflect.TypeFor[[]int]()}, false, &binding); err != nil {
				t.Fatal(err)
			}
			got, err := binding.Transformer.Transform(context.Background(), nil, item.raw)
			if item.invalid {
				if err == nil {
					t.Fatalf("invalid list accepted: %v", got)
				}
				return
			}
			ids, ok := got.([]int)
			if err != nil || !ok || ids == nil || !reflect.DeepEqual(ids, item.want) {
				t.Fatalf("empty typed list: %T %v err%v", got, got, err)
			}
		})
	}
}

func TestDefaultPrimitivePathListsHTTPAndMCP(t *testing.T) {
	type input struct {
		IDs []int `parameter:",kind=path,in=ids"`
	}
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/lists/{ids}"}}}, InputType: reflect.TypeFor[input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/lists/{ids}"})
	if !ok {
		t.Fatal("route")
	}
	bind := func(ctx context.Context, providers *requestprovider.Scope) ([]int, error) {
		t.Helper()
		injector, err := bindly.NewInjector()
		if err != nil {
			t.Fatal(err)
		}
		scope, err := injector.ForScope(providers.Providers()...)
		if err != nil {
			t.Fatal(err)
		}
		var out input
		err = scope.Bind(ctx, &out, bindly.WithPlan(route.Plan()))
		return out.IDs, err
	}
	for _, tc := range []struct {
		wire    string
		want    []int
		invalid bool
	}{{"31", []int{31}, false}, {"31,32,31", []int{31, 32, 31}, false}, {"1,,2", nil, true}, {"no", nil, true}, {"999999999999999999999999", nil, true}, {"", nil, true}} {
		t.Run("HTTP "+tc.wire, func(t *testing.T) {
			values := requestprovider.NewValues(requestprovider.WithPathParams(map[string]string{"ids": tc.wire}))
			defer values.Close()
			got, err := bind(context.Background(), values)
			if tc.invalid {
				if err == nil {
					t.Fatal("malformed path accepted")
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got%v err%v", got, err)
			}
		})
	}
	plan, err := mcpinput.NewCompiler().Compile([]mcpinput.Argument{{PublicName: "IDs", Source: bindstate.Location{Kind: "path", In: "ids"}, SourceType: reflect.TypeFor[[]int]()}})
	if err != nil {
		t.Fatal(err)
	}
	native := xexec.WithContext(context.Background(), &xexec.Context{Method: "tools/call"})
	for _, ids := range [][]int{{31}, {31, 32, 31}, {}} {
		values, err := plan.Scope(mcpinput.Arguments{"IDs": ids})
		if err != nil {
			t.Fatal(err)
		}
		got, err := bind(native, values)
		values.Close()
		if err != nil || !reflect.DeepEqual(got, ids) {
			t.Fatalf("nativegot%v want%v err%v", got, ids, err)
		}
	}
	for _, bad := range []any{31, "31,32", nil, []int(nil)} {
		if _, err := plan.Scope(mcpinput.Arguments{"IDs": bad}); err == nil {
			t.Fatalf("nonarray MCP accepted%T", bad)
		}
	}
	resource, err := plan.Scope(mcpinput.URI{Path: map[string]string{"ids": "31,32"}})
	if err != nil {
		t.Fatal(err)
	}
	defer resource.Close()
	got, err := bind(context.Background(), resource)
	if err != nil || !reflect.DeepEqual(got, []int{31, 32}) {
		t.Fatalf("resourcepath%v err%v", got, err)
	}
}

func TestDefaultPathListKeepsNativeStringsAndExistingCodecs(t *testing.T) {
	native := xexec.WithContext(context.Background(), &xexec.Context{Method: "tools/call"})
	if _, err := (&pathListTransformer{target: reflect.TypeFor[[]int]()}).Transform(native, nil, "null"); err == nil {
		t.Fatal("native null path array accepted")
	}
	got, err := (&pathListTransformer{target: reflect.TypeFor[[]string]()}).Transform(native, nil, `["one,two","three"]`)
	if err != nil || !reflect.DeepEqual(got, []string{"one,two", "three"}) {
		t.Fatalf("native strings%v err%v", got, err)
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[string](), reflect.TypeFor[[]byte](), reflect.TypeFor[[]struct{ ID int }](), reflect.TypeFor[[]*int]()} {
		binding := bindly.BindingSpec{}
		binding.Location.Kind = "path"
		if err := applyQueryList(reflect.StructField{Name: "Value", Type: typ}, false, &binding); err != nil || binding.Transformer != nil {
			t.Fatalf("existingtype changed%v err%v", typ, err)
		}
	}
	codec := &queryListTransformer{target: reflect.TypeFor[[]int]()}
	binding := bindly.BindingSpec{SourceType: reflect.TypeFor[string](), Transformer: codec}
	binding.Location.Kind = "path"
	if err := applyQueryList(reflect.StructField{Name: "IDs", Type: reflect.TypeFor[[]int]()}, false, &binding); err != nil || binding.Transformer != codec || binding.SourceType != reflect.TypeFor[string]() {
		t.Fatalf("explicit codec changed%v", err)
	}
}

type pathCountingCodec struct {
	calls int
	raw   any
}

func (c *pathCountingCodec) Value(_ context.Context, raw any, _ ...xcodec.Option) (any, error) {
	c.calls++
	c.raw = raw
	return []int{7}, nil
}
func TestExplicitPathCodecsReceiveAuthoredSourceOnce(t *testing.T) {
	for _, tc := range []struct {
		name, expr   string
		typ          reflect.Type
		raw, wantRaw any
	}{{"string", "string", reflect.TypeFor[string](), "7", "7"}, {"collection", "[]int", reflect.TypeFor[[]int](), []int{31, 32}, []int{31, 32}}} {
		t.Run(tc.name, func(t *testing.T) {
			type input struct{ IDs []int }
			codec := &pathCountingCodec{}
			component := &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/items/{ids}"}}, Parameters: []*spec.Parameter{{Name: "IDs", Source: spec.BindSource{Kind: "path", Name: "ids"}, TypeExpr: tc.expr, OutputTypeExpr: "[]int", Codec: &spec.Codec{Body: "Custom"}}}}
			compiled, err := New(Input{Component: component, InputType: reflect.TypeFor[input](), CodecFactory: namedTestCodecFactory{instances: map[string]xcodec.Instance{"Custom": codec}}}).Compile()
			if err != nil {
				t.Fatal(err)
			}
			plan, err := mcpinput.NewCompiler().Compile([]mcpinput.Argument{{PublicName: "IDs", Source: bindstate.Location{Kind: "path", In: "ids"}, SourceType: tc.typ}})
			if err != nil {
				t.Fatal(err)
			}
			providers, err := plan.Scope(mcpinput.Arguments{"IDs": tc.raw})
			if err != nil {
				t.Fatal(err)
			}
			defer providers.Close()
			injector, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			scope, err := injector.ForScope(providers.Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/items/{ids}"})
			if !ok {
				t.Fatal("route")
			}
			var result input
			ctx := xexec.WithContext(context.Background(), &xexec.Context{Method: "tools/call"})
			if err := scope.Bind(ctx, &result, bindly.WithPlan(route.Plan())); err != nil {
				t.Fatal(err)
			}
			if codec.calls != 1 || !reflect.DeepEqual(codec.raw, tc.wantRaw) || !reflect.DeepEqual(result.IDs, []int{7}) {
				t.Fatalf("codec calls%d raw%T %v out%v", codec.calls, codec.raw, codec.raw, result.IDs)
			}
		})
	}
}
