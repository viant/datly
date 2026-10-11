package generate

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type factoryAnyLinkedRow struct {
	Id  any                `sqlx:"ID"`
	Has *struct{ Id bool } `setMarker:"true" json:"-"`
}

func TestFactoryColumnDiscoveryDoesNotRelaxRuntimeOrFinalGeneration(t *testing.T) {
	for _, tc := range []struct {
		name             string
		row              any
		columnType       string
		explicit         bool
		discoveryFailure string
		strictFailure    string
	}{
		{"inferred unknown", factorySparseLinkedRow{}, "any", false, "", "requires a discovered concrete type"},
		{"explicit unknown", factorySparseLinkedRow{}, "any", true, "uneditable linked field", "uneditable linked field"},
		{"concrete compatible", factorySparseLinkedRow{}, "int", false, "", ""},
		{"concrete incompatible", factorySparseLinkedRow{}, "string", false, "uneditable linked field", "uneditable linked field"},
		{"unknown linked field", factoryAnyLinkedRow{}, "any", false, "", "requires a discovered concrete type"},
		{"discovery still requires presence", factorySparseMissingMarker{}, "any", false, "presence provider", "requires a discovered concrete type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const foreign = "example.com/app/model"
			catalog := typecatalog.NewCatalog()
			descriptor := x.NewType(reflect.TypeOf(tc.row), x.WithName("Row"), x.WithPkgPath(foreign))
			require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, descriptor))
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: "example.com/app/write", Imports: []typecatalog.PackageImport{{Alias: "model", Package: foreign}}})
			require.NoError(t, err)
			child := &spec.View{Name: "Children", TypeName: "model.Row", Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: tc.columnType}, ExplicitType: tc.explicit}}}
			identity, err := child.Identity()
			require.NoError(t, err)
			body := &spec.View{Name: "Records", TypeName: "Record", Dest: "record.go", Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}}, Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, View: child}}}
			g := New(Input{Component: &spec.Component{Name: "Records", Routes: []*spec.Route{{Method: "POST", Path: "/records", Handler: "example.com/app/write.NewRecords"}}, TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "model", Package: foreign}}}}, TargetPackage: "example.com/app/write", TypeResolver: resolver, ExternalHandler: &ExternalHandler{GeneratedContracts: true, InputShape: body, Package: "example.com/app/write", Name: "NewRecords"}, Views: ViewReferences{identity: {DescriptorKey: descriptor.Key()}}})
			before, err := json.Marshal(g.input.ExternalHandler)
			require.NoError(t, err)
			_, err = g.ColumnDiscoveryInputType()
			assertFactoryBoundaryError(t, err, tc.discoveryFailure)
			require.False(t, g.input.columnDiscovery, "discovery must not change the reusable generator")
			_, err = g.RuntimeInputType()
			assertFactoryBoundaryError(t, err, tc.strictFailure)
			_, err = g.RuntimeOutputType()
			assertFactoryBoundaryError(t, err, tc.strictFailure)
			_, err = g.Plan()
			assertFactoryBoundaryError(t, err, tc.strictFailure)
			after, err := json.Marshal(g.input.ExternalHandler)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after), "planning must not change authored destinations or columns")
			actual, err := resolver.Descriptor(descriptor.Key())
			require.NoError(t, err)
			require.Equal(t, descriptor.Type, actual.Type, "discovery must preserve catalog runtime authority")
		})
	}
}

func assertFactoryBoundaryError(t *testing.T, err error, failure string) {
	t.Helper()
	if failure == "" {
		require.NoError(t, err)
	} else {
		require.ErrorContains(t, err, failure)
	}
}
