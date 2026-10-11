package tool

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/mcp/tool/testdata/customjson"
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema/jsonschema"
	"github.com/viant/x"
)

func wireInput(t *testing.T, input reflect.Type, param *spec.Parameter) (*registry.InputContract, error) {
	t.Helper()
	catalog := typecatalog.NewCatalog()
	for _, typ := range []reflect.Type{reflect.TypeFor[customjson.RequestWire](), reflect.TypeFor[customjson.OtherWire](), reflect.TypeFor[customjson.Request](), reflect.TypeFor[customjson.CustomWire]()} {
		if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
			t.Fatal(err)
		}
	}
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[customjson.OtherWire](), x.WithName("RequestWire"), x.WithPkgPath("example.com/another"))); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Register(typecatalog.TypeOriginPackage, &x.Type{Name: "SourceOnly", PkgPath: "example.com/source"}); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/requests"}}}
	if param != nil {
		component.Parameters = []*spec.Parameter{param}
	}
	compiled, err := handlercompiler.New(handlercompiler.Input{Component: component, InputType: input, TypeLookup: resolver.Type}).Compile()
	if err != nil {
		return nil, err
	}
	return compiled.Input, nil
}

func compileWireTool(input *registry.InputContract) (*Plan, error) {
	route, _ := input.ForRoute(spec.RouteRef{Method: "POST", Path: "/requests"})
	return NewCompiler().Compile(Input{Contract: route, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "requests"}})
}

func TestCustomJSONWireAuthority(t *testing.T) {
	type noAuthority struct {
		Body *customjson.Request `parameter:",kind=body"`
	}
	missing, err := wireInput(t, reflect.TypeFor[noAuthority](), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = compileWireTool(missing); err == nil || !strings.Contains(err.Error(), "authored wire authority") {
		t.Fatalf("missing authority: %v", err)
	}
	input, err := wireInput(t, reflect.TypeFor[customjson.Input](), &spec.Parameter{Name: "Body", Source: spec.BindSource{Kind: "body"}, Required: boolPtr(true)})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := compileWireTool(input)
	if err != nil {
		t.Fatal(err)
	}
	property := plan.Metadata().InputSchema.Properties["request"]
	properties := property["properties"].(map[string]interface{})
	if property["type"] != "object" || !reflect.DeepEqual(properties["lookup"], map[string]interface{}{}) || schemaObjectNode(properties["options"].(map[string]interface{}))["properties"].(map[string]interface{})["labels"] == nil {
		t.Fatalf("wire shape: %#v", property)
	}
	if property["$defs"] == nil {
		t.Fatal("recursive wire definitions lost")
	}
	arg := plan.Arguments()[0]
	if arg.SourceType() != reflect.TypeFor[*customjson.Request]() || arg.DestinationType() != reflect.TypeFor[*customjson.Request]() || !arg.Required() {
		t.Fatalf("binding identity changed: %+v", arg)
	}
	optional, _ := input.ForRoute(spec.RouteRef{Method: "POST", Path: "/requests"})
	optional, err = optional.WithOptionalBindings("Body")
	if err != nil {
		t.Fatal(err)
	}
	optionalPlan, err := NewCompiler().Compile(Input{Contract: optional, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "requests"}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(optionalPlan.Metadata().InputSchema.Properties["request"]["type"], []string{"object", "null"}) || len(optionalPlan.Metadata().InputSchema.Required) != 0 {
		t.Fatal("optional pointer/null policy changed")
	}
}

func TestCustomJSONWireAuthorityRejectsInvalidDeclarations(t *testing.T) {
	const pkg = "github.com/viant/datly/mcp/tool/testdata/customjson."
	type plain struct {
		Body customjson.OtherWire `parameter:",kind=body"`
	}
	type anonymous struct {
		Body *customjson.Request `parameter:",kind=body" anonymous:"true"`
	}
	type absent struct {
		Body *customjson.Request `parameter:",kind=body"`
	}
	for _, tc := range []struct {
		name   string
		input  reflect.Type
		expr   string
		schema *spec.WireSchema
		want   string
	}{
		{"unresolved", reflect.TypeFor[absent](), pkg + "Missing", nil, "linked"},
		{"ambiguous", reflect.TypeFor[absent](), "RequestWire", nil, "ambiguous"},
		{"synthetic", reflect.TypeFor[absent](), "example.com/source.SourceOnly", nil, "linked"},
		{"pointer", reflect.TypeFor[absent](), "*" + pkg + "RequestWire", nil, "non-pointer"},
		{"slice", reflect.TypeFor[absent](), "[]" + pkg + "RequestWire", nil, "non-pointer"},
		{"map", reflect.TypeFor[absent](), "map[string]" + pkg + "RequestWire", nil, "non-pointer"},
		{"ordinary", reflect.TypeFor[plain](), pkg + "RequestWire", nil, "custom JSON source"},
		{"cycle", reflect.TypeFor[absent](), pkg + "Request", nil, "cyclic"},
		{"custom representation", reflect.TypeFor[absent](), pkg + "CustomWire", nil, "authored wire authority"},
		{"conflict", reflect.TypeFor[customjson.Input](), pkg + "OtherWire", nil, "conflicting"},
		{"scalar conflict", reflect.TypeFor[customjson.Input](), pkg + "RequestWire", &spec.WireSchema{Type: "string"}, "conflicts"},
		{"anonymous authority", reflect.TypeFor[anonymous](), pkg + "RequestWire", nil, "direct named"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := wireInput(t, tc.input, &spec.Parameter{Name: "Body", Source: spec.BindSource{Kind: "body"}, WireTypeExpr: tc.expr, WireSchema: tc.schema})
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "Body") {
				t.Fatalf("got %v", err)
			}
		})
	}
	input, err := wireInput(t, reflect.TypeFor[anonymous](), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = compileWireTool(input); err == nil || !strings.Contains(err.Error(), "direct named") {
		t.Fatalf("anonymous bypass: %v", err)
	}
}

func boolPtr(value bool) *bool { return &value }

type anonymousTextValue struct{ Value string }

func (anonymousTextValue) MarshalText() ([]byte, error) { panic("discovery invoked text marshaler") }

type anonymousTextPointer struct{ Value string }

func (*anonymousTextPointer) MarshalText() ([]byte, error) {
	panic("discovery invoked pointer text marshaler")
}

type anonymousTextDecoder struct{ Value string }

func (*anonymousTextDecoder) UnmarshalText([]byte) error { panic("discovery invoked text decoder") }

func TestAnonymousBodyRejectsTextMethodAuthority(t *testing.T) {
	for _, source := range []reflect.Type{reflect.TypeFor[anonymousTextValue](), reflect.TypeFor[anonymousTextPointer](), reflect.TypeFor[anonymousTextDecoder]()} {
		for _, pointer := range []bool{false, true} {
			t.Run(source.Name()+map[bool]string{false: "_value", true: "_pointer"}[pointer], func(t *testing.T) {
				if _, err := (jsonschema.Reflector{}).Compile(jsonschema.Request{Type: source}); err == nil || !strings.Contains(err.Error(), "authored wire authority") {
					t.Fatalf("lower authority: %v", err)
				}
				bodyType := source
				if pointer {
					bodyType = reflect.PointerTo(source)
				}
				inputType := reflect.StructOf([]reflect.StructField{{Name: "Body", Type: bodyType, Tag: `parameter:",kind=body" anonymous:"true"`}})
				input, err := wireInput(t, inputType, nil)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = compileWireTool(input); err == nil || !strings.Contains(err.Error(), "direct named body argument") || !strings.Contains(err.Error(), "Body") {
					t.Fatalf("anonymous text authority bypass: %v", err)
				}
			})
		}
	}
}
