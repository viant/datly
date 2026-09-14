package typecatalog

import (
	"go/ast"
	"reflect"
	"strings"
	"testing"

	x "github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

type packageSample struct{ Package string }
type generatedSample struct{ Generated string }
type dqlSample struct{ DQL string }

func TestCatalogRejectsDifferentMethodImportScope(t *testing.T) {
	source := &x.Type{Name: "Row", PkgPath: "example.com/rows", SynteticType: &smodel.Type{
		Name: "Row", PkgPath: "example.com/rows",
		MethodImports: map[string]map[string]*smodel.ImportRef{"Apply": {"h": {Path: "example.com/first"}}},
		MethodsAST:    []*ast.FuncDecl{{Name: ast.NewIdent("Apply"), Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{{Type: &ast.SelectorExpr{X: ast.NewIdent("h"), Sel: ast.NewIdent("Value")}}}}}}},
	}}
	other, err := CloneDescriptor(source)
	if err != nil {
		t.Fatal(err)
	}
	other.SynteticType.MethodImports["Apply"]["h"].Path = "example.com/other"
	catalog := NewCatalog()
	if err := catalog.Register(TypeOriginPackage, source); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Register(TypeOriginPackage, other); err == nil {
		t.Fatal("different method scope accepted for same authority")
	}
	if err := catalog.Register(TypeOriginPackage, source); err != nil {
		t.Fatalf("same descriptor rejected: %v", err)
	}
}

func TestCatalogResolveAppliesSelectedAuthority(t *testing.T) {
	keyOptions := []x.Option{x.WithName("Sample"), x.WithPkgPath("example.com/demo")}
	packageType := x.NewType(reflect.TypeOf(packageSample{}), keyOptions...)
	generatedType := x.NewType(reflect.TypeOf(generatedSample{}), keyOptions...)
	dqlType := x.NewType(reflect.TypeOf(dqlSample{}), keyOptions...)
	catalog := NewCatalog()
	for _, registration := range []struct {
		origin TypeOrigin
		typ    *x.Type
	}{
		{TypeOriginGenerated, generatedType},
		{TypeOriginDQL, dqlType},
		{TypeOriginPackage, packageType},
	} {
		if err := catalog.Register(registration.origin, registration.typ); err != nil {
			t.Fatalf("Register(%s) error = %v", registration.origin, err)
		}
	}

	if actual, ok, err := catalog.Resolve(PackageAuthority, packageType.Key()); err != nil || !ok || actual.Type != packageType.Type {
		t.Fatalf("package authority resolved %#v, want package type", actual)
	}
	if actual, ok, err := catalog.Resolve(TranscribeAuthority, packageType.Key()); err != nil || !ok || actual.Type != dqlType.Type {
		t.Fatalf("transcribe authority resolved %#v, want DQL type", actual)
	}
}

func TestCatalogPackageAuthorityPrefersGeneratedTypeOverDQLMetadata(t *testing.T) {
	keyOptions := []x.Option{x.WithName("GeneratedOnly"), x.WithPkgPath("example.com/demo")}
	generatedType := x.NewType(reflect.TypeOf(generatedSample{}), keyOptions...)
	dqlType := x.NewType(reflect.TypeOf(dqlSample{}), keyOptions...)
	catalog := NewCatalog()
	if err := catalog.Register(TypeOriginDQL, dqlType); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Register(TypeOriginGenerated, generatedType); err != nil {
		t.Fatal(err)
	}
	actual, ok, err := catalog.Resolve(PackageAuthority, generatedType.Key())
	if err != nil || !ok || actual.Type != generatedType.Type {
		t.Fatalf("package authority resolved %#v, %v; want generated type", actual, err)
	}
}

func TestCatalogRegistryIsDeterministicSnapshot(t *testing.T) {
	type first struct{ Value int }
	type second struct{ Value string }
	catalog := NewCatalog()
	firstType := x.NewType(reflect.TypeOf(first{}), x.WithName("Value"), x.WithPkgPath("example.com/demo"))
	secondType := x.NewType(reflect.TypeOf(second{}), x.WithName("Second"), x.WithPkgPath("example.com/demo"))
	if err := catalog.Register(TypeOriginPackage, firstType); err != nil {
		t.Fatal(err)
	}
	snapshot, err := catalog.Registry(PackageAuthority)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Register(TypeOriginPackage, secondType); err != nil {
		t.Fatal(err)
	}
	if actual := snapshot.Lookup(secondType.Key()); actual != nil {
		t.Fatalf("existing snapshot changed after registration: %#v", actual)
	}
	latest, err := catalog.Registry(PackageAuthority)
	if err != nil {
		t.Fatal(err)
	}
	if actual := latest.Lookup(secondType.Key()); actual == nil || actual.Type != secondType.Type {
		t.Fatalf("new snapshot resolved %#v, want newly registered type", actual)
	}
}

func TestCatalogRegisterAllIsAtomic(t *testing.T) {
	catalog := NewCatalog()
	keyOptions := []x.Option{x.WithName("Conflict"), x.WithPkgPath("example.com/demo")}
	if err := catalog.Register(TypeOriginPackage, x.NewType(reflect.TypeOf(packageSample{}), keyOptions...)); err != nil {
		t.Fatal(err)
	}
	newType := x.NewType(reflect.TypeOf(generatedSample{}), x.WithName("New"), x.WithPkgPath("example.com/demo"))
	conflict := x.NewType(reflect.TypeOf(generatedSample{}), keyOptions...)
	if err := catalog.RegisterAll(TypeOriginPackage, newType, conflict); err == nil {
		t.Fatal("expected batch conflict")
	}
	if _, ok, err := catalog.Resolve(PackageAuthority, newType.Key()); err != nil || ok {
		t.Fatalf("failed batch registered partial type: ok=%v err=%v", ok, err)
	}
}

func TestCatalogCloneIsDetached(t *testing.T) {
	catalog := NewCatalog()
	typ := x.NewType(reflect.TypeOf(packageSample{}), x.WithName("Sample"), x.WithPkgPath("example.com/demo"))
	if err := catalog.Register(TypeOriginPackage, typ); err != nil {
		t.Fatal(err)
	}
	cloned, err := catalog.Clone()
	if err != nil {
		t.Fatal(err)
	}
	extra := x.NewType(reflect.TypeOf(generatedSample{}), x.WithName("Extra"), x.WithPkgPath("example.com/demo"))
	if err = cloned.Register(TypeOriginPackage, extra); err != nil {
		t.Fatal(err)
	}
	if _, ok, resolveErr := catalog.Resolve(PackageAuthority, extra.Key()); resolveErr != nil || ok {
		t.Fatalf("clone mutation reached source: ok=%v err=%v", ok, resolveErr)
	}
}

func TestCatalogDetachesRegisteredAndResolvedDescriptors(t *testing.T) {
	type sample struct{}
	source := x.NewType(reflect.TypeOf(sample{}), x.WithName("Sample"), x.WithPkgPath("example.com/demo"))
	catalog := NewCatalog()
	if err := catalog.Register(TypeOriginPackage, source); err != nil {
		t.Fatal(err)
	}
	source.Name = "Changed"

	first, ok, err := catalog.Resolve(PackageAuthority, "example.com/demo.Sample")
	if err != nil || !ok || first.Name != "Sample" {
		t.Fatalf("Resolve() = %#v, %v, %v", first, ok, err)
	}
	first.Name = "Mutated"
	snapshot, err := catalog.Registry(PackageAuthority)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Lookup("example.com/demo.Sample").Name = "SnapshotMutation"
	second, ok, err := catalog.Resolve(PackageAuthority, "example.com/demo.Sample")
	if err != nil || !ok || second.Name != "Sample" {
		t.Fatalf("resolved descriptor mutated catalog: %#v, %v, %v", second, ok, err)
	}
}

func TestCatalogDetachesSyntheticAST(t *testing.T) {
	declaration := &smodel.Type{
		Name: "Record", PkgPath: "example.com/demo",
		TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Record"), Type: &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{{
			Names: []*ast.Ident{ast.NewIdent("ID")}, Type: ast.NewIdent("int"),
		}}}}},
	}
	catalog := NewCatalog()
	if err := catalog.RegisterPackage(TypeOriginGenerated, &smodel.Package{
		Name: "demo", PkgPath: "example.com/demo", Types: []*smodel.Type{declaration},
	}); err != nil {
		t.Fatal(err)
	}
	declaration.TypeSpec.Type.(*ast.StructType).Fields.List[0].Names[0].Name = "SourceMutation"

	first, ok, err := catalog.Resolve(PackageAuthority, "example.com/demo.Record")
	if err != nil || !ok || syntheticFirstField(first) != "ID" {
		t.Fatalf("Resolve() = %#v, %v, %v", first, ok, err)
	}
	first.SynteticType.TypeSpec.Type.(*ast.StructType).Fields.List[0].Names[0].Name = "ResolvedMutation"
	snapshot, err := catalog.Registry(PackageAuthority)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Lookup("example.com/demo.Record").SynteticType.TypeSpec.Type.(*ast.StructType).Fields.List[0].Names[0].Name = "SnapshotMutation"

	again, ok, err := catalog.Resolve(PackageAuthority, "example.com/demo.Record")
	if err != nil || !ok || syntheticFirstField(again) != "ID" {
		t.Fatalf("synthetic AST mutation leaked into catalog: %#v, %v, %v", again, ok, err)
	}
}

func TestCatalogClearsDescriptorCachesBeforeRegistration(t *testing.T) {
	field := ast.NewIdent("ID")
	synthetic := &smodel.Type{
		Name: "Record", PkgPath: "example.com/demo",
		TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Record"), Type: &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{{
			Names: []*ast.Ident{field}, Type: ast.NewIdent("int"),
		}}}}},
	}
	descriptor := &x.Type{PkgPath: "example.com/demo", Name: "Before", SynteticType: synthetic}
	_ = descriptor.Key()
	_ = synthetic.Body()
	descriptor.Name = "Record"
	field.Name = "CurrentID"

	catalog := NewCatalog()
	if err := catalog.Register(TypeOriginGenerated, descriptor); err != nil {
		t.Fatal(err)
	}
	resolved, ok, err := catalog.Resolve(PackageAuthority, "example.com/demo.Record")
	if err != nil || !ok || resolved == nil {
		t.Fatalf("Resolve() = %#v, %v, %v", resolved, ok, err)
	}
	if resolved.Key() != "example.com/demo.Record" || !strings.Contains(resolved.SynteticType.Body(), "CurrentID") {
		t.Fatalf("stale descriptor cache survived registration: key=%q body=%q", resolved.Key(), resolved.SynteticType.Body())
	}
}

func TestCatalogNormalizesASTObjectsBeforeIdempotenceCheck(t *testing.T) {
	name := ast.NewIdent("ID")
	name.Obj = &ast.Object{Kind: ast.Var, Name: "ID"}
	descriptor := &x.Type{
		PkgPath: "example.com/demo", Name: "Record",
		SynteticType: &smodel.Type{
			Name: "Record", PkgPath: "example.com/demo",
			TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Record"), Type: &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{{
				Names: []*ast.Ident{name}, Type: ast.NewIdent("int"),
			}}}}},
		},
	}
	catalog := NewCatalog()
	if err := catalog.Register(TypeOriginGenerated, descriptor); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Register(TypeOriginGenerated, descriptor); err != nil {
		t.Fatalf("equivalent parser-owned AST was not idempotent: %v", err)
	}
}

func syntheticFirstField(descriptor *x.Type) string {
	if descriptor == nil || descriptor.SynteticType == nil || descriptor.SynteticType.TypeSpec == nil {
		return ""
	}
	structure, ok := descriptor.SynteticType.TypeSpec.Type.(*ast.StructType)
	if !ok || structure.Fields == nil || len(structure.Fields.List) == 0 || len(structure.Fields.List[0].Names) == 0 {
		return ""
	}
	return structure.Fields.List[0].Names[0].Name
}

func TestCatalogRejectsInvalidAuthority(t *testing.T) {
	catalog := NewCatalog()
	if _, _, err := catalog.Resolve("unknown", "example.com/demo.Value"); err == nil {
		t.Fatal("expected Resolve authority error")
	}
	if _, err := catalog.Registry("unknown"); err == nil {
		t.Fatal("expected Registry authority error")
	}
}

func TestCatalogRejectsInvalidRegistration(t *testing.T) {
	catalog := NewCatalog()
	if err := catalog.Register("unknown", x.NewType(reflect.TypeOf(packageSample{}))); err == nil {
		t.Fatal("expected unknown origin error")
	}
	if err := catalog.Register(TypeOriginPackage, nil); err == nil {
		t.Fatal("expected nil type error")
	}
	first := x.NewType(reflect.TypeOf(packageSample{}), x.WithName("Duplicate"), x.WithPkgPath("example.com/demo"))
	second := x.NewType(reflect.TypeOf(dqlSample{}), x.WithName("Duplicate"), x.WithPkgPath("example.com/demo"))
	if err := catalog.Register(TypeOriginPackage, first); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Register(TypeOriginPackage, second); err == nil {
		t.Fatal("expected conflicting same-origin registration error")
	}
}

func TestCatalogRejectsSyntheticMethodAndTypeParameterConflicts(t *testing.T) {
	newDescriptor := func(constraint string, result string) *x.Type {
		return &x.Type{PkgPath: "example.com/demo", Name: "Generic", SynteticType: &smodel.Type{
			PkgPath: "example.com/demo", Name: "Generic",
			TypeSpec:   &ast.TypeSpec{Name: ast.NewIdent("Generic"), Type: &ast.StructType{}},
			TypeParams: []smodel.TypeParam{{Name: "T", Constraint: &smodel.Basic{Name: constraint}}},
			Methods: smodel.MethodSet{Value: []smodel.Method{{Name: "Value", Type: smodel.Func{
				Results: []smodel.Field{{Type: &smodel.Basic{Name: result}}},
			}}}},
		}}
	}
	for _, conflicting := range []*x.Type{
		newDescriptor("comparable", "string"),
		newDescriptor("any", "int"),
	} {
		catalog := NewCatalog()
		if err := catalog.Register(TypeOriginGenerated, newDescriptor("any", "string")); err != nil {
			t.Fatal(err)
		}
		if err := catalog.Register(TypeOriginGenerated, conflicting); err == nil {
			t.Fatal("expected synthetic definition conflict")
		}
	}
}
