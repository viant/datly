package bootstrap

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	selectorModel "github.com/viant/datly/internal/selectorcompanion/model"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
)

func selectorFixtureRoot(t *testing.T, modulePath string) (string, string) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "selectorcompanion")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module "+modulePath+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile("testdata/selectorcompanion/fixture.go")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "fixture.go"), source, 0600); err != nil {
		t.Fatal(err)
	}
	return root, modulePath + "/selectorcompanion"
}
func assertCompanionMetadata(t *testing.T, c *spec.Component) {
	t.Helper()
	if c.SelectorCompanion == nil {
		t.Fatal("genuine holder association missing")
	}
	got := map[string]bool{}
	for _, p := range c.Parameters {
		if p.QuerySelector != nil {
			got[p.Name] = true
		}
	}
	if len(got) != 2 || !got["Fields"] || !got["Page"] {
		t.Fatalf("selector declaration roundtrip=%v", got)
	}
	clone := c.Clone()
	if clone.SelectorCompanion != c.SelectorCompanion {
		t.Fatal("immutable association lost across native clone")
	}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "SelectorCompanion") || strings.Contains(string(raw), "DatlyQuerySelectors") {
		t.Fatal("transient association serialized as authoring option")
	}
	first, err := c.SelectorCompanion.Companion()
	if err != nil {
		t.Fatal(err)
	}
	first.Name = "Changed"
	again, err := clone.SelectorCompanion.Companion()
	if err != nil || again.Name == "Changed" {
		t.Fatal("association exposed mutable source descriptor")
	}
}
func TestSelectorCompanionGenuineSourceDiscoveryPreservesDeclarations(t *testing.T) {
	root, pkg := selectorFixtureRoot(t, "example.com/source-selector")
	routes, err := (PackageDiscovery{BaseDir: root, Include: []string{pkg}}).Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 || routes[0].linkedHolderType != nil {
		t.Fatalf("source fixture was not genuinely unlinked: routes=%+v expectedPackage=%s", routes, pkg)
	}
	loaded, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(root), "selectorcompanion")
	if err != nil {
		t.Fatal(err)
	}
	catalog := typecatalog.NewCatalog()
	for _, typ := range loaded.Types {
		if err = catalog.RegisterAll(typecatalog.TypeOriginPackage, &x.Type{Name: typ.Name, PkgPath: loaded.PkgPath, SynteticType: typ}); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: pkg, DefaultPackage: pkg})
	if err != nil {
		t.Fatal(err)
	}
	groups, err := GroupPackageComponentSources(routes, resolver)
	if err != nil {
		t.Fatal(err)
	}
	component, err := groups[0].ResolveDescriptors(resolver)
	if err != nil {
		t.Fatal(err)
	}
	assertCompanionMetadata(t, component)
}
func TestSelectorCompanionReflectedAndPackageReloadPreserveDeclarations(t *testing.T) {
	root, pkg := selectorFixtureRoot(t, "github.com/viant/datly/bootstrap/testdata")
	routes, err := (PackageDiscovery{BaseDir: root, Include: []string{pkg}, Holders: []any{fixture.Linked}, RequireLinked: true}).Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	groups, err := GroupPackageComponentSources(routes, nil)
	if err != nil {
		t.Fatal(err)
	}
	component, err := groups[0].ResolveDescriptors(nil)
	if err != nil {
		t.Fatal(err)
	}
	assertCompanionMetadata(t, component)
	reflected, err := ReflectSelectedPackages([]string{pkg}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reflected.Components) != 1 {
		t.Fatal("exact reflected holder missing")
	}
	compiled, err := reflected.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	assertCompanionMetadata(t, compiled)
	artifact, err := BuildArtifact(ArtifactInput{Component: compiled, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output]()})
	if err != nil {
		t.Fatal(err)
	}
	route, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/selector-records"})
	if !ok || route.SelectorPlan() == nil || len(route.Fields()) != 1 || route.Fields()[0].Path() != "ID" {
		t.Fatal("canonical public input binding was replaced or extended")
	}
	if reflect.TypeFor[fixture.Input]().NumField() != 2 || reflect.TypeFor[fixture.InputHas]().NumField() != 1 {
		t.Fatal("original public Input/Has shape changed")
	}

}

func TestForgedSelectorAssociationCannotGrantMissingFields(t *testing.T) {
	pkg := reflect.TypeFor[fixture.Component]().PkgPath()
	reflected, err := ReflectSelectedPackages([]string{pkg}, nil)
	if err != nil {
		t.Fatal(err)
	}
	component, err := reflected.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		alter func(*spec.Component)
		input reflect.Type
	}{
		{"different route", func(c *spec.Component) { c.Routes[0].Path = "/foreign" }, reflect.TypeFor[fixture.Input]()},
		{"different component", func(c *spec.Component) { c.Key.Name = "Foreign"; c.Name = "Foreign" }, reflect.TypeFor[fixture.Input]()},
		{"different package", func(c *spec.Component) { c.Key.Scope = "example.com/foreign" }, reflect.TypeFor[fixture.Input]()},
		{"different actual input", func(*spec.Component) {}, reflect.TypeFor[fixture.Row]()},
		{"current handler owner", func(c *spec.Component) { c.Routes[0].Handler = "foreign.Handler" }, reflect.TypeFor[fixture.Input]()},
		{"current mutation owner", func(c *spec.Component) { c.Settings.Mutation = "patch" }, reflect.TypeFor[fixture.Input]()},
		{"current sequence owner", func(c *spec.Component) { c.Settings.SequenceStrategy = "allocate" }, reflect.TypeFor[fixture.Input]()},
		{"current independent transaction owner", func(c *spec.Component) { c.Settings.IndependentChildTransactions = true }, reflect.TypeFor[fixture.Input]()},
		{"current imperative owner", func(c *spec.Component) { c.Settings.ComponentCallPolicy = "imperative" }, reflect.TypeFor[fixture.Input]()},
		{"current protected prefix owner", func(c *spec.Component) { c.Settings.ProtectedFlushTables = []string{"records"} }, reflect.TypeFor[fixture.Input]()},
		{"forged result descriptor", func(c *spec.Component) {
			holder, err := c.SelectorCompanion.Holder()
			if err != nil {
				t.Fatal(err)
			}
			c.SelectorCompanion, err = selectorModel.New(holder, c.SelectorCompanion.Field(), x.NewType(reflect.TypeFor[fixture.Input]()))
			if err != nil {
				t.Fatal(err)
			}
		}, reflect.TypeFor[fixture.Input]()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := component.Clone()
			tc.alter(candidate)
			_, err := BuildArtifact(ArtifactInput{Component: candidate, InputType: tc.input, OutputType: reflect.TypeFor[fixture.Output]()})
			if err == nil || !strings.Contains(err.Error(), "selector") {
				t.Fatalf("forged selector association did not fail at identity validation: %v", err)
			}
		})
	}
}
