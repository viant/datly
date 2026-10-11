package generate

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestNestedGeneratedViewQualifiedAuthority(t *testing.T) {
	const pkg = "example.com/app/records"
	for _, tc := range []struct {
		name, expression string
		reject           bool
	}{
		{"local alias", "rows.Dictionary", false},
		{"full package", pkg + ".Dictionary", false},
		{"bare", "Dictionary", false},
		{"foreign same basename", "foreign.Dictionary", true},
		{"unknown qualifier", "unknown.Dictionary", true},
		{"unexported", "rows.dictionary", true},
		{"pointer declaration", "*rows.Dictionary", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			child := &spec.View{Name: "Dictionary", TypeName: tc.expression, Columns: []*spec.Column{{Name: "Caption", Type: spec.TypeRef{Name: "string"}}}}
			component := &spec.Component{Name: "Records", TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "rows", Package: pkg}, {Alias: "foreign", Package: "example.com/other"}}}, RootView: &spec.View{Name: "Records", Relations: []*spec.Relation{{Name: "Dictionary", Holder: "Dictionary", Cardinality: spec.CardinalityOne, View: child}}}}
			before, err := json.Marshal(component)
			require.NoError(t, err)
			plan, err := New(Input{Component: component, TargetPackage: pkg}).Plan()
			if tc.reject {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Len(t, plan.Views, 2)
				require.Equal(t, "Dictionary", plan.Views[1].Name)
			}
			after, err := json.Marshal(component)
			require.NoError(t, err)
			require.JSONEq(t, string(before), string(after), "qualified authoring must remain immutable")
		})
	}
}

func TestNestedGeneratedViewQualifiedCollision(t *testing.T) {
	const pkg = "example.com/app/records"
	component := &spec.Component{Name: "Records", TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "rows", Package: pkg}}}, RootView: &spec.View{Name: "Records", Relations: []*spec.Relation{
		{Name: "First", Holder: "First", View: &spec.View{Name: "First", TypeName: "rows.Dictionary", Columns: []*spec.Column{{Name: "Caption", Type: spec.TypeRef{Name: "string"}}}}},
		{Name: "Second", Holder: "Second", View: &spec.View{Name: "Second", TypeName: pkg + ".Dictionary", Columns: []*spec.Column{{Name: "Caption", Type: spec.TypeRef{Name: "int"}}}}},
	}}}
	_, err := New(Input{Component: component, TargetPackage: pkg}).Plan()
	require.Error(t, err, "two spellings of the same canonical row cannot conceal conflicting contracts")
}

func TestNestedGeneratedViewQualifierRequiresPackage(t *testing.T) {
	component := &spec.Component{Name: "Records", RootView: &spec.View{Name: "Records", Relations: []*spec.Relation{{Name: "Dictionary", Holder: "Dictionary", View: &spec.View{Name: "Dictionary", TypeName: "rows.Dictionary"}}}}}
	_, err := New(Input{Component: component}).Plan()
	require.ErrorContains(t, err, "requires a generated package")
}
