package generate

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type nativeComparisonBytes []byte

func TestPreviousComparisonUsesNativeTypeResolver(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[nativeComparisonBytes](), x.WithPkgPath("example.com/contract"), x.WithName("Bytes"))); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan := &Plan{Package: "example.com/patch", Imports: []spec.ImportSpec{{Alias: "wire", Package: "example.com/contract"}}, Views: []ViewPlan{{
		Name: "Root", Type: "Root", Ownership: ViewGenerated, SetMarkerFields: []string{"Value"}, Fields: []Field{
			{Name: "Value", Type: "wire.Bytes", Tag: `sqlx:"VALUE"`},
			{Name: "Has", Type: "*RootHas", Tag: `setMarker:"true" sqlx:"-"`},
		},
	}}}
	owner := &planResolver{input: Input{TypeResolver: resolver, nativeMutationTargetSelected: true}, plan: plan}
	if err := owner.resolvePreviousPresence(); err != nil {
		t.Fatal(err)
	}
	if plan.EntitySupport == nil {
		t.Fatal("native writer did not generate comparison support")
	}
	source, err := plan.EntitySupport.source(plan.PackageName())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(source, "presenceSlices.Equal(r.Value, prev.Value)") || strings.Contains(source, "r.Value != prev.Value") {
		t.Fatalf("native imported type authority was not retained:\n%s", source)
	}
	if plan.Views[0].Fields[0].Type != "wire.Bytes" {
		t.Fatal("comparison generation rewrote the public field type")
	}
}
