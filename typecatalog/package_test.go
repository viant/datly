package typecatalog

import (
	"go/ast"
	"testing"

	smodel "github.com/viant/x/syntetic/model"
)

func TestCatalogRegisterPackageRegistersSyntheticTypes(t *testing.T) {
	declared := &smodel.Type{Name: "Input", TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Input")}}
	pkg := &smodel.Package{PkgPath: "example.com/generated", Name: "generated", Types: []*smodel.Type{declared}}
	catalog := NewCatalog()
	if err := catalog.RegisterPackage(TypeOriginGenerated, pkg); err != nil {
		t.Fatalf("RegisterPackage() error = %v", err)
	}

	typ, ok, err := catalog.Resolve(PackageAuthority, "example.com/generated.Input")
	if err != nil || !ok {
		t.Fatalf("Resolve() = %#v, %v, %v", typ, ok, err)
	}
	if typ.Type != nil || typ.SynteticType == nil || typ.SynteticType == declared || typ.SynteticType.Name != declared.Name {
		t.Fatalf("registered type = %#v, want synthetic declaration", typ)
	}
	declared.Name = "Changed"
	typ.SynteticType.Name = "Mutated"
	again, ok, err := catalog.Resolve(PackageAuthority, "example.com/generated.Input")
	if err != nil || !ok || again.SynteticType == nil || again.SynteticType.Name != "Input" {
		t.Fatalf("synthetic descriptor mutation leaked into catalog: %#v, %v, %v", again, ok, err)
	}
}

func TestCatalogRegisterPackageRejectsMissingIdentity(t *testing.T) {
	catalog := NewCatalog()
	if err := catalog.RegisterPackage(TypeOriginGenerated, &smodel.Package{}); err == nil {
		t.Fatal("expected missing package path error")
	}
	if err := catalog.RegisterPackage(TypeOriginGenerated, &smodel.Package{
		PkgPath: "example.com/generated", Types: []*smodel.Type{{}},
	}); err == nil {
		t.Fatal("expected missing type name error")
	}
}

func TestCatalogRegisterPackageAtomicallyReplacesGeneratedTypes(t *testing.T) {
	oldInput := &smodel.Type{Name: "Input"}
	removedOutput := &smodel.Type{Name: "Output"}
	catalog := NewCatalog()
	if err := catalog.RegisterPackage(TypeOriginGenerated, &smodel.Package{
		PkgPath: "example.com/generated", Types: []*smodel.Type{oldInput, removedOutput},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := catalog.Registry(PackageAuthority)
	if err != nil {
		t.Fatal(err)
	}

	newInput := &smodel.Type{Name: "Input"}
	if err := catalog.RegisterPackage(TypeOriginGenerated, &smodel.Package{
		PkgPath: "example.com/generated", Types: []*smodel.Type{newInput},
	}); err != nil {
		t.Fatalf("regeneration failed: %v", err)
	}
	current, ok, err := catalog.Resolve(PackageAuthority, "example.com/generated.Input")
	if err != nil || !ok || current.SynteticType == nil || current.SynteticType.Name != newInput.Name {
		t.Fatalf("current generated type = %#v, %v, %v", current, ok, err)
	}
	if previous := snapshot.Lookup("example.com/generated.Input"); previous == nil || previous.SynteticType == nil || previous.SynteticType.Name != oldInput.Name {
		t.Fatalf("existing snapshot changed: %#v", previous)
	}
	if _, ok, err := catalog.Resolve(PackageAuthority, "example.com/generated.Output"); err != nil || ok {
		t.Fatalf("removed generated type still resolves: %v, %v", ok, err)
	}

	if err := catalog.RegisterPackage(TypeOriginGenerated, &smodel.Package{
		PkgPath: "example.com/generated",
		Types:   []*smodel.Type{{Name: "Input"}, {Name: ""}},
	}); err == nil {
		t.Fatal("expected invalid package error")
	}
	current, ok, err = catalog.Resolve(PackageAuthority, "example.com/generated.Input")
	if err != nil || !ok || current.SynteticType == nil || current.SynteticType.Name != newInput.Name {
		t.Fatalf("failed registration changed catalog: %#v, %v, %v", current, ok, err)
	}
}

func TestCatalogRegisterPackageRejectsForeignAndDuplicateTypes(t *testing.T) {
	catalog := NewCatalog()
	if err := catalog.RegisterPackage(TypeOriginGenerated, &smodel.Package{
		PkgPath: "example.com/generated",
		Types:   []*smodel.Type{{Name: "Input", PkgPath: "example.com/foreign"}},
	}); err == nil {
		t.Fatal("expected foreign package error")
	}
	if err := catalog.RegisterPackage(TypeOriginGenerated, &smodel.Package{
		PkgPath: "example.com/generated",
		Types:   []*smodel.Type{{Name: "Input"}, {Name: "Input"}},
	}); err == nil {
		t.Fatal("expected duplicate type error")
	}
}
