package transcribe

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/testdata/linkedpredicate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

const predicateTestModule = "github.com/viant/datly/transcribe/testdata"
const predicateTestPackage = predicateTestModule + "/linkedpredicate"

func TestPredicateDependenciesUseEachDeclarationContext(t *testing.T) {
	for _, tc := range []struct {
		name, imports, reference, scope, want string
	}{
		{name: "first alias", imports: "#import('security', 'example.com/first')", reference: "security.Check", scope: "example.com/reader", want: "example.com/first"},
		{name: "second alias", imports: "#import('security', 'example.com/second')", reference: "security.Check", scope: "example.com/reader", want: "example.com/second"},
		{name: "local declaration", reference: "Check", scope: "example.com/reader", want: "example.com/reader"},
		{name: "absolute without scope", reference: "example.com/security.Check", want: "example.com/security"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			packages := map[string]bool{}
			require.NoError(t, collectPredicatePackages(packages, dql.PrepareSource(predicateDiscoverySource(tc.imports, tc.reference)), tc.scope))
			require.Equal(t, map[string]bool{tc.want: true}, packages)
		})
	}
	packages := map[string]bool{}
	require.NoError(t, collectPredicatePackages(packages, dql.PrepareSource("SELECT 1"), ""))
	require.Empty(t, packages)
}

func predicateDiscoveryWorkspace(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	writeSourceFile(t, base, "go.mod", "module "+predicateTestModule+"\n\ngo 1.25.0\n")
	data, err := os.ReadFile("testdata/linkedpredicate/predicate.go")
	require.NoError(t, err)
	writeSourceFile(t, base, "linkedpredicate/predicate.go", string(data))
	return base
}

func predicateDiscoverySource(imports, name string) string {
	return imports + `
#setting($_ = $route('/records', 'GET'))
#set($_ = $Minimum<int>(query/min).WithPredicate(0, 'handler', '` + name + `'))
SELECT id FROM records ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}`
}

func compilePredicateDiscovery(t *testing.T, discovery *Discovery, text string, inMemory bool) (*Result, error) {
	t.Helper()
	if inMemory {
		return discovery.CompileSource(context.Background(), &Source{Scope: predicateTestModule + "/reader", Name: "Records", Text: text})
	}
	writeSourceFile(t, discovery.BaseDir, "reader/Records.dql", text)
	project, err := discovery.Compile(context.Background())
	if err != nil {
		return nil, err
	}
	require.Len(t, project.Components, 1)
	return project.Components[0], nil
}

func TestDiscoveryLoadsPredicateTypeDependencies(t *testing.T) {
	for _, inMemory := range []bool{false, true} {
		for _, tc := range []struct {
			name, imports, reference string
			registered               bool
		}{
			{name: "fully qualified registry", reference: predicateTestPackage + ".Threshold", registered: true},
			{name: "fully qualified linked", reference: predicateTestPackage + ".Threshold"},
			{name: "import alias", imports: "#import('security', '" + predicateTestPackage + "')", reference: "security.Threshold", registered: true},
		} {
			t.Run(tc.name+map[bool]string{false: "/files", true: "/source"}[inMemory], func(t *testing.T) {
				base := predicateDiscoveryWorkspace(t)
				registry := x.NewRegistry()
				if tc.registered {
					registry.Register(x.NewType(reflect.TypeFor[linkedpredicate.Threshold]()))
				}
				catalog := typecatalog.NewCatalog()
				result, err := compilePredicateDiscovery(t, &Discovery{BaseDir: base, Include: []string{predicateTestModule + "/reader"}, Registry: registry, Types: catalog}, predicateDiscoverySource(tc.imports, tc.reference), inMemory)
				require.NoError(t, err)
				require.Equal(t, predicateTestPackage+".Threshold", result.Component.Parameters[0].Predicates[0].Args[0])
				descriptor, found, err := result.Source.Types.Resolve(typecatalog.PackageAuthority, predicateTestPackage+".Threshold")
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, reflect.TypeFor[linkedpredicate.Threshold](), descriptor.Type)
				require.NotNil(t, descriptor.SynteticType.TypeSpec, "retain source declaration metadata")
				_, found, err = catalog.Resolve(typecatalog.PackageAuthority, predicateTestPackage+".Threshold")
				require.NoError(t, err)
				require.False(t, found, "discovery must not publish into the caller's catalog")
			})
		}
	}
}

func TestPredicateDependenciesAreNotDiscoveryOrResourceRoots(t *testing.T) {
	base := predicateDiscoveryWorkspace(t)
	writeSourceFile(t, base, "linkedpredicate/component.go", "package linkedpredicate\n"+`
import xdatly "github.com/viant/xdatly"
type Input struct{}
type Output struct{}
type Holder struct {
    Route xdatly.Component[Input, Output] `+"`component:\"Invalid\"`"+`
}`)
	writeSourceFile(t, base, "linkedpredicate/.datly-gen.json", `{"version":5,"owner":"Other","resources":{"namespace":"unused","files":["missing.sql"]}}`)
	result, err := compilePredicateDiscovery(t, &Discovery{BaseDir: base, Include: []string{predicateTestModule + "/reader"}}, predicateDiscoverySource("", predicateTestPackage+".Threshold"), false)
	require.NoError(t, err)
	require.Equal(t, "Records", result.Component.Name)
}

func TestPredicateDiscoveryRegistryPrecedenceAndFailureIsolation(t *testing.T) {
	for _, tc := range []struct {
		name, reference, wantError string
		explicit, existing         reflect.Type
	}{
		{name: "explicit precedence", reference: "Threshold", explicit: reflect.TypeFor[availablePredicate]()},
		{name: "conflicting identities", reference: "Threshold", explicit: reflect.TypeFor[availablePredicate](), existing: reflect.TypeFor[linkedpredicate.Threshold](), wantError: "different compiled identity"},
		{name: "incompatible explicit identity", reference: "Threshold", explicit: reflect.TypeFor[incompatiblePredicate](), wantError: "does not implement predicate.Handler"},
		{name: "missing type", reference: "Absent", wantError: "was not found"},
		{name: "unlinked type", reference: "SourceOnly", wantError: "not linked into this binary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := predicateDiscoveryWorkspace(t)
			writeSourceFile(t, base, "linkedpredicate/unlinked.go", `package linkedpredicate
import (
    "context"
    "github.com/viant/xdatly/predicate"
)
type SourceOnly struct{}
func (*SourceOnly) Compute(context.Context, any) (*predicate.Criteria, error) { return nil, nil }
`)
			registry := x.NewRegistry()
			if tc.explicit != nil {
				registry.Register(x.NewType(tc.explicit, x.WithPkgPath(predicateTestPackage), x.WithName("Threshold")))
			}
			catalog := typecatalog.NewCatalog()
			if tc.existing != nil {
				require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(tc.existing)))
			}
			result, err := compilePredicateDiscovery(t, &Discovery{BaseDir: base, Include: []string{predicateTestModule + "/reader"}, Registry: registry, Types: catalog}, predicateDiscoverySource("", predicateTestPackage+"."+tc.reference), false)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
				actual, _, err := result.Source.Types.ResolveRuntimeType(typecatalog.PackageAuthority, predicateTestPackage+".Threshold")
				require.NoError(t, err)
				require.Equal(t, tc.explicit, actual)
			}
			actual, _, err := catalog.ResolveRuntimeType(typecatalog.PackageAuthority, predicateTestPackage+".Threshold")
			require.NoError(t, err)
			require.Equal(t, tc.existing, actual)
			_, found, err := catalog.Resolve(typecatalog.PackageAuthority, predicateTestPackage+".SourceOnly")
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}
