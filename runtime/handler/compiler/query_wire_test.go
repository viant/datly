package compiler

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
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
		{"required bool query", "query", "*bool", &required, reflect.TypeFor[bool](), "", false},
		{"named bool query", "query", "*bool", nil, reflect.TypeFor[namedQueryBool](), "", false},
		{"pointer bool query", "query", "*bool", nil, reflect.TypeFor[*bool](), "", false},
		{"bool form exclusion", "form", "*bool", nil, reflect.TypeFor[bool](), "", false},
		{"bool path exclusion", "path", "*bool", nil, reflect.TypeFor[bool](), "", false},
		{"bool body exclusion", "body", "*bool", nil, reflect.TypeFor[bool](), "", false},
		{"bool codec exclusion", "query", "*bool", nil, reflect.TypeFor[bool](), `codec:"json"`, false},
		{"bool wireType exclusion", "query", "*bool", nil, reflect.TypeFor[bool](), `wireType:"model.Value"`, false},
		{"plain optional form", "form", "string", nil, reflect.TypeFor[string](), "", false},
		{"required form", "form", "*string", &required, reflect.TypeFor[string](), "", false},
		{"named form", "form", "*string", nil, reflect.TypeFor[namedFormString](), "", false},
		{"int form", "form", "*int", nil, reflect.TypeFor[int](), "", false},
		{"bool form", "form", "*bool", nil, reflect.TypeFor[bool](), "", false},
		{"file form", "form", "*string", nil, reflect.TypeFor[[]*multipart.FileHeader](), "", false},
		{"codec form", "form", "*string", nil, reflect.TypeFor[string](), `codec:"json"`, false},
		{"wireType form", "form", "*string", nil, reflect.TypeFor[string](), `wireType:"model.Value"`, false},
		{"path", "path", "*string", nil, reflect.TypeFor[string](), "", false},
		{"optional scalar default", "query", "string", nil, reflect.TypeFor[string](), `value:"ui"`, false},
		{"required", "query", "*string", &required, reflect.TypeFor[string](), "", false},
		{"integer", "query", "*int", nil, reflect.TypeFor[int](), "", false},
		{"collection", "query", "[]string", nil, reflect.TypeFor[[]string](), "", false},
		{"body", "body", "*string", nil, reflect.TypeFor[string](), "", false},
		{"codec", "query", "*string", nil, reflect.TypeFor[string](), `codec:"json"`, false},
		{"wireType", "query", "*string", nil, reflect.TypeFor[string](), `wireType:"model.Value"`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := bindly.BindingSpec{DataType: tc.dataType, Location: bs.Location{Kind: tc.kind}, Required: tc.required, DefaultValue: "ui"}
			require.NoError(t, applyExplicitScalarWire(reflect.StructField{Type: tc.typ, Tag: tc.tag}, &b))
			p, _ := b.Extension.(*spec.Parameter)
			if tc.want {
				schemaType := "string"
				if tc.typ == reflect.TypeFor[bool]() {
					schemaType = "boolean"
				}
				require.Equal(t, &spec.WireSchema{Type: schemaType, Nullable: true}, p.WireSchema)
			} else {
				require.Nil(t, p)
			}
			require.Nil(t, b.SourceType)
			require.Nil(t, b.Transformer)
			require.Equal(t, "ui", b.DefaultValue)
			require.Equal(t, tc.required, b.Required)
		})
	}
	explicit := &spec.Parameter{WireSchema: &spec.WireSchema{Type: "string", Format: "date"}}
	b := bindly.BindingSpec{DataType: "*string", Location: bs.Location{Kind: "query"}, Extension: explicit}
	require.NoError(t, applyExplicitScalarWire(reflect.StructField{Type: reflect.TypeFor[string]()}, &b))
	require.Same(t, explicit, b.Extension)
	require.False(t, explicit.WireSchema.Nullable)
}

type boolWireTransformer struct{}

func (boolWireTransformer) Transform(_ context.Context, _ locator.Resolver, input any) (any, error) {
	return input, nil
}

func TestExplicitBoolWirePreservesCompetingAuthority(t *testing.T) {
	field := reflect.StructField{Type: reflect.TypeFor[bool]()}
	for _, b := range []bindly.BindingSpec{
		{DataType: "*bool", Location: bs.Location{Kind: "query"}, SourceType: reflect.TypeFor[bool]()},
		{DataType: "*bool", Location: bs.Location{Kind: "query"}, Transformer: boolWireTransformer{}},
	} {
		require.NoError(t, applyExplicitScalarWire(field, &b))
		require.Nil(t, b.Extension)
	}
	p := &spec.Parameter{WireSchema: &spec.WireSchema{Type: "boolean", Nullable: false}}
	b := bindly.BindingSpec{DataType: "*bool", Location: bs.Location{Kind: "query"}, Extension: p}
	require.NoError(t, applyExplicitScalarWire(field, &b))
	require.Same(t, p, b.Extension)
	require.False(t, p.WireSchema.Nullable)
}
