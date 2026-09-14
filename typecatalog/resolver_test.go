package typecatalog

import (
	"errors"
	"go/ast"
	"reflect"
	"testing"

	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

func TestResolverUsesPackageImportAndGlobalRules(t *testing.T) {
	type localOrder struct{}
	type customer struct{}
	catalog := NewCatalog()
	for _, typ := range []*x.Type{
		x.NewType(reflect.TypeOf(localOrder{}), x.WithName("Order"), x.WithPkgPath("example.com/app/orders")),
		x.NewType(reflect.TypeOf(customer{}), x.WithName("Customer"), x.WithPkgPath("example.com/models")),
	} {
		if err := catalog.Register(TypeOriginPackage, typ); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := NewResolver(catalog, PackageAuthority, &ResolutionContext{
		PackagePath: "example.com/app/orders",
		Imports:     []PackageImport{{Alias: "model", Package: "example.com/models"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	for expression, expected := range map[string]string{
		"Order":          "example.com/app/orders.Order",
		"model.Customer": "example.com/models.Customer",
		"[]*Customer":    "example.com/models.Customer",
	} {
		actual, err := resolver.Resolve(expression)
		if err != nil || actual != expected {
			t.Fatalf("Resolve(%q) = %q, %v; want %q", expression, actual, err, expected)
		}
	}
}

func TestResolverUsesOuterAliasForGenericType(t *testing.T) {
	catalog := NewCatalog()
	if err := catalog.RegisterAll(TypeOriginPackage,
		&x.Type{PkgPath: "example.com/models", Name: "Page[example.com/other.Item]"},
		&x.Type{PkgPath: "example.com/other", Name: "Item"},
	); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	resolver, err := NewResolver(catalog, PackageAuthority, &ResolutionContext{
		Imports: []PackageImport{
			{Alias: "model", Package: "example.com/models"},
			{Alias: "other", Package: "example.com/other"},
		},
	})
	if err != nil {
		t.Fatalf("NewResolver() error = %v", err)
	}
	actual, err := resolver.Resolve("model.Page[other.Item]")
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if actual != "example.com/models.Page[example.com/other.Item]" {
		t.Fatalf("Resolve() = %q", actual)
	}
}

func TestResolverFallsBackToGenericDeclaration(t *testing.T) {
	catalog := NewCatalog()
	if err := catalog.RegisterAll(TypeOriginPackage,
		&x.Type{PkgPath: "example.com/models", Name: "Page"},
		&x.Type{PkgPath: "example.com/other", Name: "Item"},
	); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	resolver, err := NewResolver(catalog, PackageAuthority, &ResolutionContext{
		Imports: []PackageImport{
			{Alias: "model", Package: "example.com/models"},
			{Alias: "other", Package: "example.com/other"},
		},
	})
	if err != nil {
		t.Fatalf("NewResolver() error = %v", err)
	}
	actual, err := resolver.Resolve("model.Page[other.Item]")
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if actual != "example.com/models.Page" {
		t.Fatalf("Resolve() = %q", actual)
	}
}

func TestResolverReturnsDetachedSyntheticDescriptor(t *testing.T) {
	catalog := NewCatalog()
	if err := catalog.RegisterPackage(TypeOriginGenerated, &smodel.Package{
		Name: "generated", PkgPath: "example.com/generated",
		Types: []*smodel.Type{{Name: "Event", TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Event"), Type: &ast.StructType{}}}},
	}); err != nil {
		t.Fatal(err)
	}
	resolver, err := NewResolver(catalog, TranscribeAuthority, &ResolutionContext{PackagePath: "example.com/generated"})
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := resolver.Descriptor("Event")
	if err != nil || descriptor == nil || descriptor.Type != nil || descriptor.SynteticType == nil {
		t.Fatalf("Descriptor() = %#v, %v", descriptor, err)
	}
	descriptor.Name = "Changed"
	descriptor.SynteticType.Name = "Changed"
	descriptor.SynteticType.TypeSpec.Type = ast.NewIdent("Changed")
	again, err := resolver.Descriptor("Event")
	if err != nil || again == nil || again.Name != "Event" || again.SynteticType == nil || again.SynteticType.Name != "Event" {
		t.Fatalf("descriptor mutation leaked into resolver: %#v, %v", again, err)
	}
	if _, ok := again.SynteticType.TypeSpec.Type.(*ast.StructType); !ok {
		t.Fatalf("descriptor AST mutation leaked into resolver: %#v", again.SynteticType.TypeSpec.Type)
	}
}

func TestNewResolverRejectsInvalidAuthority(t *testing.T) {
	if _, err := NewResolver(NewCatalog(), "unknown", nil); err == nil {
		t.Fatal("expected invalid authority error")
	}
}

func TestResolverRejectsAmbiguousGlobalType(t *testing.T) {
	type first struct{}
	type second struct{}
	catalog := NewCatalog()
	for _, typ := range []*x.Type{
		x.NewType(reflect.TypeOf(first{}), x.WithName("Record"), x.WithPkgPath("example.com/a")),
		x.NewType(reflect.TypeOf(second{}), x.WithName("Record"), x.WithPkgPath("example.com/b")),
	} {
		if err := catalog.Register(TypeOriginPackage, typ); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := NewResolver(catalog, PackageAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = resolver.Resolve("Record")
	var ambiguity *AmbiguityError
	if !errors.As(err, &ambiguity) || len(ambiguity.Candidates) != 2 {
		t.Fatalf("Resolve() error = %v", err)
	}
}

func TestPackageAuthorityShadowsImportedContractNames(t *testing.T) {
	catalog := NewCatalog()
	if err := catalog.RegisterAll(TypeOriginPackage, &x.Type{PkgPath: "example.com/app", Name: "Input"}, &x.Type{PkgPath: "example.com/hooks", Name: "Input"}); err != nil {
		t.Fatal(err)
	}
	context := &ResolutionContext{PackagePath: "example.com/app", Imports: []PackageImport{{Alias: "hooks", Package: "example.com/hooks"}}}
	resolver, err := NewResolver(catalog, PackageAuthority, context)
	if err != nil {
		t.Fatal(err)
	}
	for expression, want := range map[string]string{"Input": "example.com/app.Input", "hooks.Input": "example.com/hooks.Input"} {
		got, err := resolver.Resolve(expression)
		if err != nil || got != want {
			t.Fatalf("%s: %s %v", expression, got, err)
		}
	}
	resolver, err = NewResolver(catalog, TranscribeAuthority, context)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = resolver.Resolve("Input"); err == nil {
		t.Fatal("transcription ambiguity policy changed")
	}
}
