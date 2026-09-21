package typecatalog

import (
	"go/ast"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

func TestResolveRuntimeTypePreservesAuthority(t *testing.T) {
	catalog := NewCatalog()
	const key = "example.com/demo.Record"
	native := reflect.TypeOf(struct{ ID int }{})
	dql := reflect.TypeOf(struct{ Value string }{})
	require.NoError(t, catalog.Register(TypeOriginGenerated, x.NewType(native, x.WithPkgPath("example.com/demo"), x.WithName("Record"))))
	require.NoError(t, catalog.Register(TypeOriginDQL, x.NewType(dql, x.WithPkgPath("example.com/demo"), x.WithName("Record"))))
	got, found, err := catalog.ResolveRuntimeType(PackageAuthority, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, native, got)
	got, found, err = catalog.ResolveRuntimeType(TranscribeAuthority, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, dql, got)

	require.NoError(t, catalog.RegisterPackage(TypeOriginPackage, &smodel.Package{
		PkgPath: "example.com/demo", Types: []*smodel.Type{{Name: "Record"}},
	}))
	got, found, err = catalog.ResolveRuntimeType(PackageAuthority, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Nil(t, got, "synthetic package authority must not fall through to a compiled generated type")
	got, found, err = catalog.ResolveRuntimeType(PackageAuthority, "missing")
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, got)
	_, _, err = catalog.ResolveRuntimeType("invalid", key)
	require.Error(t, err)
	_, _, err = (*Catalog)(nil).ResolveRuntimeType(PackageAuthority, key)
	require.Error(t, err)
}

func TestLinkRuntimeAllEnrichesPackageDeclaration(t *testing.T) {
	catalog := NewCatalog()
	declaration := runtimeIdentityDeclaration(1)
	require.NoError(t, catalog.RegisterPackage(TypeOriginPackage, &smodel.Package{
		PkgPath: "example.com/demo", Types: []*smodel.Type{declaration},
	}))
	native := reflect.TypeOf(struct{ ID int }{})
	require.NoError(t, catalog.LinkRuntimeAll(TypeOriginPackage,
		x.NewType(native, x.WithPkgPath("example.com/demo"), x.WithName("Record"))))

	resolved, found, err := catalog.Resolve(PackageAuthority, "example.com/demo.Record")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, native, resolved.Type)
	require.Equal(t, native, resolved.SynteticType.ReflectType)
	require.Equal(t, "ID", syntheticFirstField(resolved))

	other := reflect.TypeOf(struct{ Name string }{})
	err = catalog.LinkRuntimeAll(TypeOriginPackage,
		x.NewType(other, x.WithPkgPath("example.com/demo"), x.WithName("Record")))
	require.ErrorContains(t, err, "different compiled identity")

	err = catalog.LinkRuntimeAll(TypeOriginPackage,
		x.NewType(reflect.TypeOf(struct{ Added bool }{}), x.WithPkgPath("example.com/demo"), x.WithName("Added")),
		x.NewType(other, x.WithPkgPath("example.com/demo"), x.WithName("Record")))
	require.ErrorContains(t, err, "different compiled identity")
	_, found, err = catalog.Resolve(PackageAuthority, "example.com/demo.Added")
	require.NoError(t, err)
	require.False(t, found, "failed runtime linking must remain atomic")
}

func TestPackageFilesRuntimeLinkRetainsIsolation(t *testing.T) {
	catalog := NewCatalog()
	const key = "example.com/demo.Record"
	native := reflect.TypeOf(struct{ ID int }{})
	require.NoError(t, catalog.Register(TypeOriginPackage, x.NewType(native, x.WithPkgPath("example.com/demo"), x.WithName("Record"))))
	declared := runtimeIdentityDeclaration(1)
	pkg := &smodel.Package{PkgPath: "example.com/demo", Types: []*smodel.Type{declared},
		Files: []*smodel.GoFile{{Name: "authored.go", Types: []*smodel.Type{declared}}}}
	require.NoError(t, catalog.RegisterPackageFiles(pkg, nil))
	require.Nil(t, declared.ReflectType, "linking must not mutate the input package")
	snapshot, err := catalog.Clone()
	require.NoError(t, err)

	// Refresh the authored source while preserving its existing compiled identity.
	declared.TypeSpec.Type.(*ast.StructType).Fields.List[0].Names[0].Name = "Updated"
	require.NoError(t, catalog.RegisterPackageFiles(pkg, nil))
	declared.TypeSpec.Type.(*ast.StructType).Fields.List[0].Names[0].Name = "CallerMutation"
	current, found, err := catalog.Resolve(PackageAuthority, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, native, current.Type)
	require.Equal(t, native, current.SynteticType.ReflectType)
	require.Equal(t, "Updated", syntheticFirstField(current))
	current.SynteticType.TypeSpec.Type.(*ast.StructType).Fields.List[0].Names[0].Name = "PublicMutation"
	current, found, err = catalog.Resolve(PackageAuthority, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "Updated", syntheticFirstField(current))
	old, found, err := snapshot.Resolve(PackageAuthority, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "ID", syntheticFirstField(old))
	require.Equal(t, native, old.Type)
}

func TestRuntimeIdentityLookupDoesNotCloneAST(t *testing.T) {
	catalog := NewCatalog()
	declared := runtimeIdentityDeclaration(1000)
	declared.ReflectType = reflect.TypeOf(struct{ ID int }{})
	require.NoError(t, catalog.RegisterPackage(TypeOriginPackage, &smodel.Package{
		PkgPath: "example.com/demo", Types: []*smodel.Type{declared},
	}))
	allocations := testing.AllocsPerRun(100, func() {
		got, found, err := catalog.ResolveRuntimeType(PackageAuthority, "example.com/demo.Record")
		if err != nil || !found || got != declared.ReflectType {
			t.Fatal("runtime identity changed")
		}
	})
	require.Zero(t, allocations, "identity-only reads must not clone a synthetic AST")
}

func runtimeIdentityDeclaration(count int) *smodel.Type {
	fields := make([]*ast.Field, count)
	for i := range fields {
		fields[i] = &ast.Field{Names: []*ast.Ident{ast.NewIdent("ID")}, Type: ast.NewIdent("int")}
	}
	return &smodel.Type{Name: "Record", TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Record"),
		Type: &ast.StructType{Fields: &ast.FieldList{List: fields}}}}
}

func TestResolverRuntimeIdentityDoesNotScaleWithAST(t *testing.T) {
	allocations := make([]float64, 0, 2)
	for _, fields := range []int{1, 1000} {
		catalog := NewCatalog()
		declared := runtimeIdentityDeclaration(fields)
		declared.ReflectType = reflect.TypeOf(struct{ ID int }{})
		require.NoError(t, catalog.RegisterPackage(TypeOriginPackage, &smodel.Package{
			PkgPath: "example.com/demo", Types: []*smodel.Type{declared},
		}))
		resolver, err := NewResolver(catalog, PackageAuthority, &ResolutionContext{
			Imports: []PackageImport{{Alias: "demo", Package: "example.com/demo"}},
		})
		require.NoError(t, err)
		allocations = append(allocations, testing.AllocsPerRun(100, func() {
			got, err := resolver.Type("demo.Record")
			if err != nil || got != declared.ReflectType {
				t.Fatal("runtime identity changed")
			}
		}))
		for _, expression := range []string{"demo.Record", "[]*demo.Record", "Missing"} {
			descriptor, err := resolver.Descriptor(expression)
			require.NoError(t, err)
			got, err := resolver.Type(expression)
			require.NoError(t, err)
			if descriptor == nil {
				require.Nil(t, got)
			} else {
				require.Equal(t, descriptor.Type, got)
			}
		}
		shape, err := resolver.ResolveShape("demo.Record")
		require.NoError(t, err)
		shape.Descriptor.SynteticType.TypeSpec.Type.(*ast.StructType).Fields.List[0].Names[0].Name = "Mutation"
		again, err := resolver.ResolveShape("demo.Record")
		require.NoError(t, err)
		require.Equal(t, "ID", syntheticFirstField(again.Descriptor))
	}
	// Parsing/formatting pools can vary slightly between runs; cloning this
	// graph adds thousands of allocations rather than a small constant.
	require.LessOrEqual(t, allocations[1], allocations[0]+10, "runtime lookup allocations must not grow with the AST")
}

func BenchmarkCatalogRuntimeIdentity(b *testing.B) {
	catalog := NewCatalog()
	require.NoError(b, catalog.RegisterPackage(TypeOriginPackage, &smodel.Package{
		PkgPath: "example.com/demo", Types: []*smodel.Type{runtimeIdentityDeclaration(1000)},
	}))
	for _, identityOnly := range []bool{false, true} {
		name := "descriptor"
		if identityOnly {
			name = "identity"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if identityOnly {
					_, _, _ = catalog.ResolveRuntimeType(PackageAuthority, "example.com/demo.Record")
				} else {
					_, _, _ = catalog.Resolve(PackageAuthority, "example.com/demo.Record")
				}
			}
		})
	}
}
