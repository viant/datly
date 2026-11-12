package bootstrap

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/x"
)

type declarationRow struct {
	ID int `sqlx:"id,primaryKey"`
}
type declarationInput struct {
	Rows   []*declarationRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=records,auxiliary=true,rootNullPolicy=skip-auxiliary"`
	Lookup []*declarationRow `parameter:"Lookup,kind=view,in=Lookup" view:"Lookup,table=records,auxiliary=true" sql:"SELECT id FROM records"`
}
type declarationOutput struct {
	Data []*declarationRow `parameter:"Data,kind=output,in=body"`
}

func TestCurrentViewDeclarationsRetainUnreplacedPackageAuxiliary(t *testing.T) {
	pkg := reflect.TypeFor[declarationInput]().PkgPath()
	source := &PackageComponentSource{Routes: []*RouteSource{{PackagePath: pkg, Tag: dtag.Component{Name: "Records", Method: "PATCH", Path: "/records"}}}, InputType: x.NewType(reflect.TypeFor[declarationInput]()), OutputType: x.NewType(reflect.TypeFor[declarationOutput]())}
	authored := &spec.Component{RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: "SELECT id FROM (records)"}}, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "body", Name: "data"}}}}
	component, err := source.ResolveDeclarations(nil, authored)
	require.NoError(t, err)
	require.Len(t, component.Views, 1)
	lookup := component.Views[0]
	require.Equal(t, "Lookup", lookup.Name)
	require.True(t, lookup.Auxiliary)
	require.Equal(t, "records", lookup.Source.Table)
	require.Equal(t, "SELECT id FROM records", lookup.Source.SQL)
	_, err = source.ResolveDescriptors(nil)
	require.ErrorContains(t, err, "rootNullPolicy requires")
}
