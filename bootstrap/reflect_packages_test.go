package bootstrap

import (
	"reflect"
	"slices"
	"testing"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/xdatly"
)

type ReflectedPackageInput struct {
	ID int
}

type ReflectedPackageOutput struct {
	Rows []ReflectedPackageRow
}

type ReflectedPackageRow struct {
	ID int
}

type ReflectedOrdinaryHandler struct {
	Name string
}

var reflectedOrdinaryHandlerLink = reflect.TypeFor[ReflectedOrdinaryHandler]()

var reflectedLocalWireOutputOneLink = reflectedLocalWireOutputOne()
var reflectedLocalWireOutputTwoLink = reflectedLocalWireOutputTwo()

func reflectedLocalWireOutputOne() any {
	type wireOutput struct {
		ID int
	}
	return wireOutput{}
}

func reflectedLocalWireOutputTwo() any {
	type wireOutput struct {
		Name string
	}
	return wireOutput{}
}

func TestRegisterContractTypesIgnoresFunctionLocalHelperTypes(t *testing.T) {
	_, _ = reflectedLocalWireOutputOne(), reflectedLocalWireOutputTwo()

	contracts := map[reflect.Type]bool{}
	collectContractTypes(contracts, reflect.TypeFor[ReflectedPackageInput]())
	collectContractTypes(contracts, reflect.TypeFor[ReflectedPackageOutput]())
	if contracts[reflect.TypeOf(reflectedLocalWireOutputOneLink)] || contracts[reflect.TypeOf(reflectedLocalWireOutputTwoLink)] {
		t.Fatalf("function-local helper leaked into contract closure: %+v", contracts)
	}
	if err := registerContractTypes(typecatalog.NewCatalog(), contracts); err != nil {
		t.Fatal(err)
	}
}

func TestReflectPackagesIncludesOrdinaryExportedTypes(t *testing.T) {
	_ = reflectedOrdinaryHandlerLink
	reflected, err := ReflectPackages([]string{"github.com/viant/datly/bootstrap"})
	if err != nil {
		t.Fatal(err)
	}
	const key = "github.com/viant/datly/bootstrap.ReflectedOrdinaryHandler"
	resolved, ok, err := reflected.Types.Resolve(typecatalog.PackageAuthority, key)
	if err != nil || !ok || resolved == nil || resolved.Type != reflect.TypeFor[ReflectedOrdinaryHandler]() {
		t.Fatalf("ordinary package type %s: resolved=%#v found=%v err=%v", key, resolved, ok, err)
	}
}

func TestReflectSelectedPackagesExcludesAndOrdersLinkedPackages(t *testing.T) {
	selected := []string{"github.com/viant/datly/bootstrap/connector", "github.com/viant/datly/bootstrap"}
	for range 3 {
		reflected, err := ReflectSelectedPackages(selected, []string{"github.com/viant/datly/bootstrap/connector"})
		if err != nil {
			t.Fatal(err)
		}
		if len(reflected.Packages) != 1 || reflected.Packages[0] != "github.com/viant/datly/bootstrap" {
			t.Fatalf("selected packages = %v", reflected.Packages)
		}
	}
}

func TestReflectedPackageMatch(t *testing.T) {
	for _, test := range []struct {
		name        string
		pattern     string
		packagePath string
		want        bool
	}{
		{"recursive root", "example.com/foo/...", "example.com/foo", true},
		{"recursive child", "example.com/foo/...", "example.com/foo/bar", true},
		{"recursive nested child", "example.com/foo/...", "example.com/foo/bar/baz", true},
		{"recursive sibling", "example.com/foo/...", "example.com/foobar", false},
		{"recursive sibling child", "example.com/foo/...", "example.com/foobar/baz", false},
		{"recursive parent", "example.com/foo/...", "example.com", false},
		{"trimmed recursive root", " example.com/foo/... ", "example.com/foo", true},
		{"all packages", "...", "example.com/foo", true},
		{"legacy prefix", "example.com/foo...", "example.com/foobar", true},
		{"exact", "example.com/foo", "example.com/foo", true},
		{"exact excludes children", "example.com/foo", "example.com/foo/bar", false},
		{"star", "example.com/*", "example.com/foo", true},
		{"star excludes nested children", "example.com/*", "example.com/foo/bar", false},
		{"question mark", "example.com/fo?", "example.com/foo", true},
		{"character class", "example.com/fo[op]", "example.com/foo", true},
		{"invalid wildcard", "example.com/[", "example.com/foo", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := reflectedPackageMatch(test.pattern, test.packagePath); got != test.want {
				t.Fatalf("reflectedPackageMatch(%q, %q) = %v, want %v", test.pattern, test.packagePath, got, test.want)
			}
		})
	}
}

func TestReflectedPackagePathsRecursiveIncludesLinkedRootAndChildren(t *testing.T) {
	const root = "github.com/viant/datly/bootstrap"
	selected := reflectedPackagePaths([]string{root + "/..."}, nil)
	for _, packagePath := range []string{root, root + "/connector"} {
		if !slices.Contains(selected, packagePath) {
			t.Fatalf("recursive include omitted linked package %q: %v", packagePath, selected)
		}
	}
}

func TestReflectedPackagePathsRecursiveExcludesRootAndChildren(t *testing.T) {
	selected := reflectedPackagePaths([]string{
		"example.com/foo", "example.com/foo/bar", "example.com/foobar", "example.com/foobar/baz",
	}, []string{"example.com/foo/..."})
	want := []string{"example.com/foobar", "example.com/foobar/baz"}
	if !slices.Equal(selected, want) {
		t.Fatalf("recursive exclusion selected %v, want %v", selected, want)
	}
}

func TestReflectRecursiveDoesNotDiscoverUnlinkedPackages(t *testing.T) {
	selected, err := ReflectSelectedPackages([]string{"example.invalid/unlinked/..."}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected.Packages) != 0 || len(selected.Components) != 0 {
		t.Fatalf("unlinked package supplied runtime metadata: %+v", selected)
	}
}

// The component is genuinely retained in the executable's runtime type table.
// Recursive discovery must find it without package source or manual registration.
type ReflectedRootComponent struct {
	Route xdatly.Component[ReflectedPackageInput, ReflectedPackageOutput] `component:"rootReader,path=/linked-root-reader,method=GET,internal=true"`
}

var reflectedRootComponentLink = reflect.TypeFor[ReflectedRootComponent]()

func TestReflectRecursiveSelectionIncludesLinkedRootComponent(t *testing.T) {
	const root = "github.com/viant/datly/bootstrap"
	if reflectedRootComponentLink.PkgPath() != root {
		t.Fatal("fixture is not linked in the root package")
	}
	selected, err := ReflectSelectedPackages([]string{root + "/..."}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, component := range selected.Components {
		if component.PackagePath == root && component.HolderType == "ReflectedRootComponent" && component.FieldName == "Route" {
			if component.LinkedInputType != reflect.TypeFor[ReflectedPackageInput]() || component.LinkedOutputType != reflect.TypeFor[ReflectedPackageOutput]() {
				t.Fatal("discovery lost linked input/output authority")
			}
			return
		}
	}
	t.Fatal("recursive selection omitted the component linked into the root package")
}
