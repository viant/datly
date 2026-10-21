package registry

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bs "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"mime/multipart"
	"reflect"
	"testing"
)

type namedFormString string
type namedQueryBool bool

func TestPersistedQueryStringWireBoundaries(t *testing.T) {
	required := true
	for _, tc := range []struct {
		name, kind, dataType string
		required             *bool
		typ                  reflect.Type
		tag                  reflect.StructTag
		want                 bool
	}{
		{"explicit", "query", "*string", nil, reflect.TypeFor[string](), "", true},
		{"explicit form", "form", "*string", nil, reflect.TypeFor[string](), "", true},
		{"explicit bool query", "query", "*bool", nil, reflect.TypeFor[bool](), "", true},
		{"plain optional bool", "query", "bool", nil, reflect.TypeFor[bool](), "", false},
		{"required bool query", "query", "*bool", &required, reflect.TypeFor[bool](), "", true},
		{"named bool query", "query", "*bool", nil, reflect.TypeFor[namedQueryBool](), "", false},
		{"pointer bool query", "query", "*bool", nil, reflect.TypeFor[*bool](), "", false},
		{"bool form exclusion", "form", "*bool", nil, reflect.TypeFor[bool](), "", true},
		{"bool path exclusion", "path", "*bool", nil, reflect.TypeFor[bool](), "", true},
		{"bool body exclusion", "body", "*bool", nil, reflect.TypeFor[bool](), "", false},
		{"bool codec exclusion", "query", "*bool", nil, reflect.TypeFor[bool](), `codec:"json"`, false},
		{"bool wireType exclusion", "query", "*bool", nil, reflect.TypeFor[bool](), `wireType:"model.Value"`, false},
		{"plain optional form", "form", "string", nil, reflect.TypeFor[string](), "", false},
		{"required form", "form", "*string", &required, reflect.TypeFor[string](), "", true},
		{"named form", "form", "*string", nil, reflect.TypeFor[namedFormString](), "", false},
		{"int form", "form", "*int", nil, reflect.TypeFor[int](), "", true},
		{"bool form", "form", "*bool", nil, reflect.TypeFor[bool](), "", true},
		{"file form", "form", "*string", nil, reflect.TypeFor[[]*multipart.FileHeader](), "", false},
		{"codec form", "form", "*string", nil, reflect.TypeFor[string](), `codec:"json"`, false},
		{"wireType form", "form", "*string", nil, reflect.TypeFor[string](), `wireType:"model.Value"`, false},
		{"path", "path", "*string", nil, reflect.TypeFor[string](), "", true},
		{"optional scalar default", "query", "string", nil, reflect.TypeFor[string](), `value:"ui"`, false},
		{"required", "query", "*string", &required, reflect.TypeFor[string](), "", true},
		{"integer", "query", "*int", nil, reflect.TypeFor[int](), "", true},
		{"collection", "query", "[]string", nil, reflect.TypeFor[[]string](), "", false},
		{"body", "body", "*string", nil, reflect.TypeFor[string](), "", false},
		{"codec", "query", "*string", nil, reflect.TypeFor[string](), `codec:"json"`, false},
		{"wireType", "query", "*string", nil, reflect.TypeFor[string](), `wireType:"model.Value"`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := bindly.BindingSpec{DataType: tc.dataType, Location: bs.Location{Kind: tc.kind}, Required: tc.required, DefaultValue: "ui"}
			schema := bindingScalarWireSchema(reflect.StructField{Type: tc.typ, Tag: tc.tag}, b)
			if tc.want {
				schemaType := "string"
				if tc.typ == reflect.TypeFor[bool]() {
					schemaType = "boolean"
				}
				if tc.typ == reflect.TypeFor[int]() {
					schemaType = "integer"
				}
				require.Equal(t, &spec.WireSchema{Type: schemaType, Nullable: true}, schema)
			} else {
				require.Nil(t, schema)
			}
			require.Nil(t, b.SourceType)
			require.Nil(t, b.Transformer)
			require.Equal(t, "ui", b.DefaultValue)
			require.Equal(t, tc.required, b.Required)
		})
	}
	explicit := &spec.Parameter{WireSchema: &spec.WireSchema{Type: "string", Format: "date"}}
	b := bindly.BindingSpec{DataType: "*string", Location: bs.Location{Kind: "query"}, Extension: explicit}
	require.Nil(t, bindingScalarWireSchema(reflect.StructField{Type: reflect.TypeFor[string]()}, b))
	require.Same(t, explicit, b.Extension)
	require.False(t, explicit.WireSchema.Nullable)
}

func TestScalarWireSourceOverrideIsCheckedBeforeRegistryDefaults(t *testing.T) {
	type input struct {
		Value bool `parameter:",kind=query,in=value,dataType=*bool"`
	}
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			param := &spec.Parameter{Name: "Value", TypeExpr: "*bool", Source: spec.BindSource{Kind: "query", Name: "value"}}
			binding := bindly.BindingSpec{Path: "Value", DataType: "*bool", Location: bs.Location{Kind: "query", In: "value"}, Extension: param}
			if explicit {
				binding.SourceType = reflect.TypeFor[bool]()
			}
			injector, err := bindly.NewInjector()
			require.NoError(t, err)
			plan, err := injector.CompilePlan(reflect.TypeFor[input](), binding)
			require.NoError(t, err)
			projection, err := plan.Projection()
			require.NoError(t, err)
			route := spec.RouteRef{Method: "GET", Path: "/value"}
			contract, err := NewInputContract(reflect.TypeFor[input](), projection, RouteInput{Route: route, Plan: plan, Bindings: []bindly.BindingSpec{binding}})
			require.NoError(t, err)
			fields, ok := contract.ForRoute(route)
			require.True(t, ok)
			field := fields.Fields()[0]
			if explicit {
				require.Nil(t, field.WireSchema())
			} else {
				require.Equal(t, &spec.WireSchema{Type: "boolean", Nullable: true}, field.WireSchema())
			}
			require.Same(t, param, binding.Extension)
			require.Nil(t, param.WireSchema)
			require.Equal(t, reflect.TypeFor[bool](), field.SourceType())
		})
	}
}
