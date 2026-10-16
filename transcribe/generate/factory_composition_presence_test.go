package generate

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"reflect"
	"testing"
)

type compositionNoMarker struct{ Id int }
type compositionBadMarker struct {
	Id  int
	Has bool `setMarker:"true"`
}
type compositionWrongMarker struct {
	Id  int
	Has *struct{ Other bool } `setMarker:"true" json:"-"`
}
type compositionContainer struct{ Child *compositionNoMarker }

func compositionPresenceInput(t *testing.T, row any) (Input, *spec.View) {
	t.Helper()
	const foreign = "example.com/app/model"
	catalog := typecatalog.NewCatalog()
	descriptor := x.NewType(reflect.TypeOf(row), x.WithName("Row"), x.WithPkgPath(foreign))
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, descriptor))
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: "example.com/app/write", Imports: []typecatalog.PackageImport{{Alias: "model", Package: foreign}}})
	require.NoError(t, err)
	child := &spec.View{Name: "Rows", Cardinality: spec.CardinalityMany, TypeName: "model.Row", Source: &spec.ViewSource{}, Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}}}
	identity, err := child.Identity()
	require.NoError(t, err)
	root := &spec.View{Name: "Root", Cardinality: spec.CardinalityOne, TypeName: "Root", Dest: "root.go", Source: &spec.ViewSource{}, Relations: []*spec.Relation{{Name: "Rows", Holder: "Rows", Cardinality: spec.CardinalityMany, View: child}}}
	rootID, err := root.Identity()
	require.NoError(t, err)
	return Input{Component: &spec.Component{Name: "Records", Routes: []*spec.Route{{Method: "POST", Path: "/records", Handler: "example.com/app/write.NewRecords"}}, TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "model", Package: foreign}}}}, TargetPackage: "example.com/app/write", TypeResolver: resolver, ExternalHandler: &ExternalHandler{Package: "example.com/app/write", Name: "NewRecords", GeneratedContracts: true, InputShape: root, CompositionViews: map[string]CompositionOccurrence{rootID: {TypeName: root.TypeName, Cardinality: root.Cardinality}, identity: {TypeName: child.TypeName, Cardinality: child.Cardinality}}}, Views: ViewReferences{identity: {DescriptorKey: descriptor.Key()}}}, child
}

func TestCompositionPresenceIsExactOccurrenceOnly(t *testing.T) {
	for _, tc := range []struct {
		name   string
		row    any
		change func(Input, *spec.View)
		want   string
	}{
		{"original markerless wrapper", compositionNoMarker{}, nil, ""},
		{"unknown tableless occurrence", compositionNoMarker{}, func(in Input, v *spec.View) { id, _ := v.Identity(); delete(in.ExternalHandler.CompositionViews, id) }, "presence provider"},
		{"physical conflicting proof", compositionNoMarker{}, func(in Input, v *spec.View) { v.Source.Table = "records" }, "stale, ambiguous or physical"},
		{"stale nominal proof", compositionNoMarker{}, func(in Input, v *spec.View) { v.TypeName = "model.Else" }, "stale, ambiguous or physical"},
		{"unknown proof member", compositionNoMarker{}, func(in Input, v *spec.View) {
			in.ExternalHandler.CompositionViews["view:unknown"] = CompositionOccurrence{TypeName: "Row"}
		}, "unknown member"},
		{"duplicate occurrence identity", compositionNoMarker{}, func(in Input, v *spec.View) {
			in.ExternalHandler.InputShape.Relations = append(in.ExternalHandler.InputShape.Relations, &spec.Relation{Name: "Again", Holder: "Again", Cardinality: spec.CardinalityMany, View: v})
		}, "stale, ambiguous or physical"},
		{"existing malformed provider", compositionBadMarker{}, nil, "pointer to a struct"},
		{"existing missing member", compositionWrongMarker{}, nil, "must be bool"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in, child := compositionPresenceInput(t, tc.row)
			if tc.change != nil {
				tc.change(in, child)
			}
			g := New(in)
			plan, err := g.Plan()
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
				return
			}
			require.NoError(t, err)
			for _, view := range plan.Views {
				if view.Name == "Root" {
					for _, field := range view.Fields {
						require.NotEqual(t, "Has", field.Name, "logical generated root must retain original markerless contract")
					}
				}
			}
			data, err := json.Marshal(g.input.ExternalHandler)
			require.NoError(t, err)
			require.NotContains(t, string(data), "CompositionViews")
			clone := g.input.ExternalHandler.Clone()
			for id := range clone.CompositionViews {
				delete(clone.CompositionViews, id)
			}
			require.NotEmpty(t, g.input.ExternalHandler.CompositionViews)
		})
	}
}

func TestCompositionParentDoesNotExemptPhysicalChild(t *testing.T) {
	in, wrapper := compositionPresenceInput(t, compositionContainer{})
	wrapper.Columns = nil
	wrapper.Relations = []*spec.Relation{{Name: "Child", Holder: "Child", Cardinality: spec.CardinalityOne, View: &spec.View{Name: "Physical", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}}}}}
	_, err := New(in).Plan()
	require.ErrorContains(t, err, "presence provider")
}
