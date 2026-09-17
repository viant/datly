package bootstrap

import (
	"go/ast"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

func TestArtifactCatalogRetainsLinkedSourceMetadata(t *testing.T) {
	const key = "example.com/demo.Record"
	native := reflect.TypeOf(struct{ ID int }{})
	registry := x.NewRegistry()
	registry.Register(x.NewType(native, x.WithPkgPath("example.com/demo"), x.WithName("Record")))
	builder, err := NewArtifactBuilder(registry)
	require.NoError(t, err)
	source := typecatalog.NewCatalog()
	declaration := &smodel.Type{Name: "Record", ReflectType: native,
		TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Record"), Doc: &ast.CommentGroup{
			List: []*ast.Comment{{Text: "// authored metadata"}},
		}}}
	require.NoError(t, source.RegisterPackage(typecatalog.TypeOriginPackage, &smodel.Package{
		PkgPath: "example.com/demo", Types: []*smodel.Type{declaration},
	}))
	published, err := builder.Catalog(source)
	require.NoError(t, err)
	got, found, err := published.Resolve(typecatalog.PackageAuthority, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, native, got.Type)
	require.Equal(t, "// authored metadata", got.SynteticType.TypeSpec.Doc.List[0].Text)
	got.SynteticType.TypeSpec.Doc.List[0].Text = "// caller mutation"
	// Neither a public descriptor nor a later source refresh may mutate publication.
	declaration.TypeSpec.Doc.List[0].Text = "// revised source"
	require.NoError(t, source.RegisterPackage(typecatalog.TypeOriginPackage, &smodel.Package{
		PkgPath: "example.com/demo", Types: []*smodel.Type{declaration},
	}))
	got, found, err = published.Resolve(typecatalog.PackageAuthority, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "// authored metadata", got.SynteticType.TypeSpec.Doc.List[0].Text)
	current, found, err := source.Resolve(typecatalog.PackageAuthority, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "// revised source", current.SynteticType.TypeSpec.Doc.List[0].Text)
}
