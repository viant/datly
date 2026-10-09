package typecatalog

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/x"
)

func TestExplicitImportAliasWinsOverAuthoringPackageName(t *testing.T) {
	catalog := NewCatalog()
	row := x.NewType(reflect.TypeOf(struct{ ID int }{}), x.WithName("AudienceView"), x.WithPkgPath("example.com/app/pkg/audience"))
	if err := catalog.Register(TypeOriginPackage, row); err != nil {
		t.Fatal(err)
	}
	resolver, err := NewResolver(catalog, TranscribeAuthority, &ResolutionContext{PackagePath: "example.com/app/dql/audience", PackageName: "audience", Imports: []PackageImport{{Alias: "audience", Package: "example.com/app/pkg/audience"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, expression := range []string{"audience.AudienceView", "*audience.AudienceView", "[]*audience.AudienceView"} {
		descriptor, err := resolver.Descriptor(expression)
		if err != nil || descriptor == nil || descriptor.Key() != row.Key() {
			t.Fatalf("Descriptor(%s)=%v,%v", expression, descriptor, err)
		}
		shape, err := resolver.ResolveShape(expression)
		if err != nil || shape == nil || shape.Descriptor == nil || shape.Descriptor.Key() != row.Key() {
			t.Fatalf("ResolveShape(%s)=%+v,%v", expression, shape, err)
		}
		want := strings.Replace(expression, "audience.", "example.com/app/pkg/audience.", 1)
		canonical, err := resolver.CanonicalDeclaration(expression, "example.com/app/pkg/audience")
		if err != nil || shape.Identity != want || canonical != want {
			t.Fatalf("identity=%q canonical=%q err=%v", shape.Identity, want, err)
		}
	}
}
