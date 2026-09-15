package typecatalog

import "testing"

func TestNormalizeContextDefaultsPackageIdentityAndImports(t *testing.T) {
	actual := NormalizeContext(&ResolutionContext{
		PackageDir:  `generated\\orders`,
		PackagePath: "example.com/app/orders",
		Imports:     []PackageImport{{Package: "example.com/models/customer"}},
	})
	if actual == nil {
		t.Fatal("NormalizeContext() returned nil")
	}
	if actual.PackageName != "orders" || actual.DefaultPackage != "example.com/app/orders" {
		t.Fatalf("NormalizeContext() = %+v", actual)
	}
	if len(actual.Imports) != 1 || actual.Imports[0].Alias != "customer" {
		t.Fatalf("imports = %+v", actual.Imports)
	}
}

func TestValidateContextRejectsPackageNameMismatch(t *testing.T) {
	issues := ValidateContext(&ResolutionContext{PackagePath: "example.com/app/orders", PackageName: "payments"})
	if len(issues) != 1 || issues[0].Field != "PackagePath" {
		t.Fatalf("ValidateContext() = %+v", issues)
	}
}

func TestNormalizeContextPreservesAbsolutePackageDirectory(t *testing.T) {
	actual := NormalizeContext(&ResolutionContext{PackageDir: "/workspace/generated/orders"})
	if actual == nil || actual.PackageDir != "/workspace/generated/orders" {
		t.Fatalf("NormalizeContext() = %+v", actual)
	}
}

func TestCanonicalDeclarationUsesDestinationAndExplicitImports(t *testing.T) {
	resolver, err := NewResolver(NewCatalog(), TranscribeAuthority, &ResolutionContext{PackagePath: "example.com/source", Imports: []PackageImport{{Alias: "hooks", Package: "example.com/shop/hooks"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ expression, destination, want string }{
		{"OrderRules", "example.com/generated", "example.com/generated.OrderRules"},
		{"hooks.OrderLifecycle", "example.com/generated", "example.com/shop/hooks.OrderLifecycle"},
		{"OrderRules", "example.com/external", "example.com/external.OrderRules"},
	} {
		got, err := resolver.CanonicalDeclaration(tc.expression, tc.destination)
		if err != nil || got != tc.want {
			t.Fatalf("%s: %s %v", tc.expression, got, err)
		}
	}
}
