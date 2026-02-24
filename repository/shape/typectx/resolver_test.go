package typectx

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/x"
)

type resolveFeeA struct{}
type resolveFeeB struct{}
type resolveOrder struct{}

func TestResolver_Resolve_Unqualified_DefaultPackage(t *testing.T) {
	reg := x.NewRegistry()
	reg.Register(x.NewType(reflect.TypeOf(resolveOrder{}), x.WithPkgPath("github.com/acme/mdp/performance"), x.WithName("Order")))
	resolver := NewResolver(reg, &Context{DefaultPackage: "github.com/acme/mdp/performance"})

	key, err := resolver.Resolve("Order")
	require.NoError(t, err)
	require.Equal(t, "github.com/acme/mdp/performance.Order", key)
}

func TestResolver_Resolve_AliasQualified(t *testing.T) {
	reg := x.NewRegistry()
	reg.Register(x.NewType(reflect.TypeOf(resolveOrder{}), x.WithPkgPath("github.com/acme/mdp/performance"), x.WithName("Order")))
	resolver := NewResolver(reg, &Context{
		Imports: []Import{
			{Alias: "perf", Package: "github.com/acme/mdp/performance"},
		},
	})

	key, err := resolver.Resolve("perf.Order")
	require.NoError(t, err)
	require.Equal(t, "github.com/acme/mdp/performance.Order", key)
}

func TestResolver_Resolve_Unqualified_Ambiguous(t *testing.T) {
	reg := x.NewRegistry()
	reg.Register(x.NewType(reflect.TypeOf(resolveFeeA{}), x.WithPkgPath("github.com/acme/alpha"), x.WithName("Fee")))
	reg.Register(x.NewType(reflect.TypeOf(resolveFeeB{}), x.WithPkgPath("github.com/acme/beta"), x.WithName("Fee")))
	resolver := NewResolver(reg, &Context{
		Imports: []Import{
			{Alias: "a", Package: "github.com/acme/alpha"},
			{Alias: "b", Package: "github.com/acme/beta"},
		},
	})

	key, err := resolver.Resolve("Fee")
	require.Empty(t, key)
	require.Error(t, err)
	amb, ok := err.(*AmbiguityError)
	require.True(t, ok)
	require.Equal(t, []string{
		"github.com/acme/alpha.Fee",
		"github.com/acme/beta.Fee",
	}, amb.Candidates)
}

func TestResolver_Resolve_Unqualified_GlobalUniqueFallback(t *testing.T) {
	reg := x.NewRegistry()
	reg.Register(x.NewType(reflect.TypeOf(resolveOrder{}), x.WithPkgPath("github.com/acme/shared"), x.WithName("Order")))
	resolver := NewResolver(reg, nil)

	key, err := resolver.Resolve("Order")
	require.NoError(t, err)
	require.Equal(t, "github.com/acme/shared.Order", key)
}

func TestResolver_ResolveWithProvenance(t *testing.T) {
	reg := x.NewRegistry()
	reg.Register(x.NewType(reflect.TypeOf(resolveOrder{}), x.WithPkgPath("github.com/acme/mdp/performance"), x.WithName("Order")))
	resolver := NewResolverWithProvenance(reg, &Context{DefaultPackage: "github.com/acme/mdp/performance"}, map[string]Provenance{
		"github.com/acme/mdp/performance.Order": {
			Package: "github.com/acme/mdp/performance",
			File:    "/repo/mdp/performance/order.go",
			Kind:    "resource_type",
		},
	})

	resolved, err := resolver.ResolveWithProvenance("Order")
	require.NoError(t, err)
	require.NotNil(t, resolved)
	require.Equal(t, "github.com/acme/mdp/performance.Order", resolved.ResolvedKey)
	require.Equal(t, "default_package", resolved.MatchKind)
	require.Equal(t, "/repo/mdp/performance/order.go", resolved.Provenance.File)
	require.Equal(t, "resource_type", resolved.Provenance.Kind)
}

func TestResolver_Resolve_Unqualified_PackagePath(t *testing.T) {
	reg := x.NewRegistry()
	reg.Register(x.NewType(reflect.TypeOf(resolveOrder{}), x.WithPkgPath("github.com/acme/mdp/performance"), x.WithName("Order")))
	resolver := NewResolver(reg, &Context{PackagePath: "github.com/acme/mdp/performance"})

	resolved, err := resolver.ResolveWithProvenance("Order")
	require.NoError(t, err)
	require.NotNil(t, resolved)
	require.Equal(t, "github.com/acme/mdp/performance.Order", resolved.ResolvedKey)
	require.Equal(t, "package_path", resolved.MatchKind)
}

func TestResolver_Resolve_Qualified_PackageNameFallback(t *testing.T) {
	reg := x.NewRegistry()
	reg.Register(x.NewType(reflect.TypeOf(resolveOrder{}), x.WithPkgPath("github.com/acme/mdp/performance"), x.WithName("Order")))
	resolver := NewResolver(reg, &Context{
		PackageName: "performance",
		PackagePath: "github.com/acme/mdp/performance",
	})

	key, err := resolver.Resolve("performance.Order")
	require.NoError(t, err)
	require.Equal(t, "github.com/acme/mdp/performance.Order", key)
}
