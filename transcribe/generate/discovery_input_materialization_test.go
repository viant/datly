package generate

import (
	"go/ast"
	"go/parser"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

func TestColumnDiscoveryDescriptorOnlyBoundary(t *testing.T) {
	const pkg = "example.com/imported"
	catalog := typecatalog.NewCatalog()
	register := func(name, expr string, imports ...map[string]*smodel.ImportRef) {
		parsed, err := parser.ParseExpr(expr)
		require.NoError(t, err)
		typ := &smodel.Type{PkgPath: pkg, Name: name, TypeSpec: &ast.TypeSpec{Name: ast.NewIdent(name), Type: parsed}}
		if len(imports) != 0 {
			typ.Imports = imports[0]
		}
		require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, &x.Type{PkgPath: pkg, Name: name, SynteticType: typ}))
	}
	register("Row", "struct { Child *Child; ID int `sqlx:\"record_id\" json:\"id,omitempty\"` }")
	register("Child", "struct { Caption string }")
	register("Recursive", "struct { Next *Recursive }")
	register("Missing", "struct { Next *Absent }")
	register("AliasRow", "struct { Item *shared.Child }", map[string]*smodel.ImportRef{"shared": {Path: pkg}})
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	require.NoError(t, err)
	plan := &Plan{Package: "example.com/consumer"}
	strict := newRuntimeInputMaterializer(plan, resolver)
	_, err = strict.descriptorType(pkg + ".Row")
	require.ErrorContains(t, err, "no compiled runtime type")
	discovery := newRuntimeInputMaterializer(plan, resolver)
	discovery.discoveryOnly = true
	// Consumer naming cannot supply the declaring package's Child identity.
	discovery.imports["imported"] = "example.com/unrelated"
	row, err := discovery.descriptorType(pkg + ".Row")
	require.NoError(t, err)
	id, ok := row.FieldByName("ID")
	require.True(t, ok)
	require.Equal(t, "record_id", id.Tag.Get("sqlx"))
	require.Equal(t, "id,omitempty", id.Tag.Get("json"))
	child, ok := row.FieldByName("Child")
	require.True(t, ok)
	_, ok = child.Type.Elem().FieldByName("Caption")
	require.True(t, ok)
	again, err := discovery.descriptorType(pkg + ".Row")
	require.NoError(t, err)
	require.Equal(t, row, again)
	discovery.imports["shared"] = "example.com/unrelated"
	aliasRow, err := discovery.descriptorType(pkg + ".AliasRow")
	require.NoError(t, err)
	item, ok := aliasRow.FieldByName("Item")
	require.True(t, ok)
	require.Equal(t, child.Type, item.Type, "import alias must use the source descriptor's package authority")
	descriptor, err := resolver.Descriptor(pkg + ".Row")
	require.NoError(t, err)
	require.Nil(t, descriptor.Type, "temporary type must never enter catalog authority")
	_, err = discovery.descriptorType(pkg + ".Recursive")
	require.ErrorContains(t, err, "recursive")
	_, err = discovery.descriptorType(pkg + ".Missing")
	require.ErrorContains(t, err, "was not found")
	native := reflect.TypeFor[runtimeInputCustomer]()
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(native)))
	compiledResolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	require.NoError(t, err)
	compiled := newRuntimeInputMaterializer(plan, compiledResolver)
	compiled.discoveryOnly = true
	got, err := compiled.descriptorType(native.PkgPath() + "." + native.Name())
	require.NoError(t, err)
	require.Equal(t, native, got, "compiled named identity takes precedence")
}
