package compiler

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bs "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

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
			require.NoError(t, applyExplicitQueryWire(reflect.StructField{Type: tc.typ, Tag: tc.tag}, &b))
			p, _ := b.Extension.(*spec.Parameter)
			if tc.want {
				require.Equal(t, &spec.WireSchema{Type: "string", Nullable: true}, p.WireSchema)
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
	require.NoError(t, applyExplicitQueryWire(reflect.StructField{Type: reflect.TypeFor[string]()}, &b))
	require.Same(t, explicit, b.Extension)
	require.False(t, explicit.WireSchema.Nullable)
}
