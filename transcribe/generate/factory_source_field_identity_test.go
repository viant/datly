package generate

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"reflect"
	"testing"
)

type factoryDeclaredAcronymRow struct {
	RecordIDs []int
	Has       *struct{ RecordIDs bool } `setMarker:"true" json:"-"`
}

func TestFactoryTablelessLinkedFieldKeepsDeclaredAcronym(t *testing.T) {
	const foreign = "example.com/app/model"
	catalog := typecatalog.NewCatalog()
	descriptor := x.NewType(reflect.TypeFor[factoryDeclaredAcronymRow](), x.WithName("Row"), x.WithPkgPath(foreign))
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, descriptor))
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: "example.com/app/write", Imports: []typecatalog.PackageImport{{Alias: "model", Package: foreign}}})
	require.NoError(t, err)
	for _, physical := range []bool{false, true} {
		t.Run(map[bool]string{false: "logical exact source", true: "physical unchanged"}[physical], func(t *testing.T) {
			child := &spec.View{Name: "Children", Cardinality: spec.CardinalityMany, TypeName: "model.Row", Source: &spec.ViewSource{}, Columns: []*spec.Column{{Name: "RecordIDs", Source: "RecordIDs", Type: spec.TypeRef{Name: "[]int"}, ExplicitType: true}}}
			if physical {
				child.Source.Table = "records"
			}
			identity, err := child.Identity()
			require.NoError(t, err)
			body := &spec.View{Name: "Records", TypeName: "Record", Dest: "record.go", Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}}, Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, View: child}}}
			proofs := map[string]CompositionOccurrence{}
			if !physical {
				proofs[identity] = CompositionOccurrence{TypeName: child.TypeName, Cardinality: child.Cardinality}
			}
			g := New(Input{Component: &spec.Component{Name: "Records", Routes: []*spec.Route{{Method: "POST", Path: "/records", Handler: "example.com/app/write.NewRecords"}}, TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "model", Package: foreign}}}}, TargetPackage: "example.com/app/write", TypeResolver: resolver, ExternalHandler: &ExternalHandler{GeneratedContracts: true, InputShape: body, CompositionViews: proofs, Package: "example.com/app/write", Name: "NewRecords"}, Views: ViewReferences{identity: {DescriptorKey: descriptor.Key()}}})
			_, err = g.Plan()
			if physical {
				require.ErrorContains(t, err, "RecordIdS")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
