package compile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/view/state"
)

func TestResolveComponentNamespace(t *testing.T) {
	dqlRoot := "/repo/dql"
	source := "/repo/dql/platform/tvaffiliatestation/tvaffiliatestation.dql"
	assert.Equal(t, "platform/acl/auth", resolveComponentNamespace("../acl/auth", source, dqlRoot))
	assert.Equal(t, "platform/acl/auth", resolveComponentNamespace("GET:/v1/api/platform/acl/auth", source, dqlRoot))
	assert.Equal(t, "platform/acl/auth", resolveComponentNamespace("v1/api/platform/acl/auth", source, dqlRoot))
}

func TestDQLToRouteNamespace(t *testing.T) {
	ns, ok := dqlToRouteNamespace("/repo/dql/platform/tvaffiliatestation/tvaffiliatestation.dql")
	require.True(t, ok)
	assert.Equal(t, "platform/tvaffiliatestation/tvaffiliatestation", ns)
}

func TestSourceRoots_CustomLayout(t *testing.T) {
	layout := compilePathLayout{
		dqlMarker:      "/sqlsrc/",
		routesRelative: "config/routes",
	}
	platformRoot, routesRoot, dqlRoot, ok := sourceRootsWithLayout("/repo/sqlsrc/platform/agency/agency.dql", layout)
	require.True(t, ok)
	assert.Equal(t, "/repo", filepath.ToSlash(platformRoot))
	assert.Equal(t, "/repo/config/routes", filepath.ToSlash(routesRoot))
	assert.Equal(t, "/repo/sqlsrc", filepath.ToSlash(dqlRoot))

	ns, ok := dqlToRouteNamespaceWithLayout("/repo/sqlsrc/platform/agency/agency.dql", layout)
	require.True(t, ok)
	assert.Equal(t, "platform/agency/agency", ns)
}

func TestAppendComponentTypes(t *testing.T) {
	temp := t.TempDir()
	dqlDir := filepath.Join(temp, "dql", "platform", "tvaffiliatestation")
	routesDir := filepath.Join(temp, "repo", "dev", "Datly", "routes", "platform", "acl")
	require.NoError(t, os.MkdirAll(dqlDir, 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(routesDir, "auth"), 0o755))
	require.NoError(t, os.MkdirAll(routesDir, 0o755))
	sourcePath := filepath.Join(dqlDir, "tvaffiliatestation.dql")
	require.NoError(t, os.WriteFile(sourcePath, []byte("SELECT 1"), 0o644))

	authYAML := `Resource:
  Types:
    - Name: Input
      DataType: "*Input"
      Package: acl/auth
      ModulePath: github.vianttech.com/viant/platform/pkg/platform/acl/auth
  Parameters:
    - In:
        Kind: component
        Name: GET:/v1/api/platform/acl/user
Routes:
  - Handler:
      OutputType: acl/auth.Output
`
	userYAML := `Resource:
  Types:
    - Name: UserView
      DataType: "struct{Id int;}"
      Package: acl
      ModulePath: github.vianttech.com/viant/platform/pkg/platform/acl
`
	require.NoError(t, os.WriteFile(filepath.Join(routesDir, "auth", "auth.yaml"), []byte(authYAML), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(routesDir, "user.yaml"), []byte(userYAML), 0o644))

	result := &plan.Result{
		States: []*plan.State{
			{Parameter: state.Parameter{Name: "Auth", In: &state.Location{Kind: state.KindComponent, Name: "../acl/auth"}}},
		},
	}
	appendComponentTypes(&shape.Source{Path: sourcePath, DQL: "#set($Auth = $component<../acl/auth>())"}, result)
	require.Len(t, result.Types, 2)
	names := map[string]bool{}
	for _, item := range result.Types {
		names[item.Name] = true
	}
	assert.True(t, names["Input"])
	assert.True(t, names["UserView"])
	assert.Equal(t, "*Output", result.States[0].Schema.DataType)
}

func TestAppendComponentTypes_MissingComponentRoute(t *testing.T) {
	temp := t.TempDir()
	dqlDir := filepath.Join(temp, "dql", "platform", "sample")
	require.NoError(t, os.MkdirAll(dqlDir, 0o755))
	sourcePath := filepath.Join(dqlDir, "sample.dql")
	dql := "#set($Auth = $component<../acl/missing>())\nSELECT 1"
	require.NoError(t, os.WriteFile(sourcePath, []byte(dql), 0o644))
	result := &plan.Result{
		States: []*plan.State{{Parameter: state.Parameter{Name: "Auth", In: &state.Location{Kind: state.KindComponent, Name: "../acl/missing"}}}},
	}
	diags := appendComponentTypes(&shape.Source{Path: sourcePath, DQL: dql}, result)
	require.NotEmpty(t, diags)
	assert.Equal(t, "DQL-COMP-ROUTE-MISSING", diags[0].Code)
	assert.GreaterOrEqual(t, diags[0].Span.Start.Line, 1)
	assert.GreaterOrEqual(t, diags[0].Span.Start.Char, 1)
}

func TestAppendComponentTypes_TypeCollisionEmitsDiagnostic(t *testing.T) {
	temp := t.TempDir()
	dqlDir := filepath.Join(temp, "dql", "platform", "tvaffiliatestation")
	routesDir := filepath.Join(temp, "repo", "dev", "Datly", "routes", "platform", "acl")
	require.NoError(t, os.MkdirAll(dqlDir, 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(routesDir, "auth"), 0o755))
	sourcePath := filepath.Join(dqlDir, "tvaffiliatestation.dql")
	require.NoError(t, os.WriteFile(sourcePath, []byte("SELECT 1"), 0o644))

	authYAML := `Resource:
  Types:
    - Name: Input
      DataType: "*Input"
      Package: acl/auth
      ModulePath: github.vianttech.com/viant/platform/pkg/platform/acl/auth
`
	require.NoError(t, os.WriteFile(filepath.Join(routesDir, "auth", "auth.yaml"), []byte(authYAML), 0o644))

	result := &plan.Result{
		States: []*plan.State{
			{Parameter: state.Parameter{Name: "Auth", In: &state.Location{Kind: state.KindComponent, Name: "../acl/auth"}}},
		},
		Types: []*plan.Type{
			{
				Name:       "Input",
				DataType:   "*Input",
				Package:    "campaign/patch",
				ModulePath: "github.vianttech.com/viant/platform/pkg/platform/campaign/patch",
			},
		},
	}
	diags := appendComponentTypes(&shape.Source{Path: sourcePath, DQL: "#set($Auth = $component<../acl/auth>())"}, result)
	require.NotEmpty(t, diags)
	var found bool
	for _, item := range diags {
		if item != nil && item.Code == dqldiag.CodeCompTypeCollision {
			found = true
			break
		}
	}
	assert.True(t, found)
	require.Len(t, result.Types, 1)
	assert.Equal(t, "campaign/patch", result.Types[0].Package)
}

func TestAppendComponentTypes_InvalidRouteYAMLEmitsDiagnostic(t *testing.T) {
	temp := t.TempDir()
	dqlDir := filepath.Join(temp, "dql", "platform", "sample")
	routesDir := filepath.Join(temp, "repo", "dev", "Datly", "routes", "platform", "acl")
	require.NoError(t, os.MkdirAll(dqlDir, 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(routesDir, "auth"), 0o755))

	sourcePath := filepath.Join(dqlDir, "sample.dql")
	dql := "#set($Auth = $component<../acl/auth>())\nSELECT 1"
	require.NoError(t, os.WriteFile(sourcePath, []byte(dql), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(routesDir, "auth", "auth.yaml"), []byte("Resource:\n  Types: ["), 0o644))

	result := &plan.Result{
		States: []*plan.State{{Parameter: state.Parameter{Name: "Auth", In: &state.Location{Kind: state.KindComponent, Name: "../acl/auth"}}}},
	}
	diags := appendComponentTypes(&shape.Source{Path: sourcePath, DQL: dql}, result)
	require.NotEmpty(t, diags)
	assert.Equal(t, dqldiag.CodeCompRouteInvalid, diags[0].Code)
}

func TestAppendComponentTypes_InvalidRouteYAMLDedupedForRepeatedStates(t *testing.T) {
	temp := t.TempDir()
	dqlDir := filepath.Join(temp, "dql", "platform", "sample")
	routesDir := filepath.Join(temp, "repo", "dev", "Datly", "routes", "platform", "acl")
	require.NoError(t, os.MkdirAll(dqlDir, 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(routesDir, "auth"), 0o755))

	sourcePath := filepath.Join(dqlDir, "sample.dql")
	dql := "#set($Auth1 = $component<../acl/auth>())\n#set($Auth2 = $component<../acl/auth>())\nSELECT 1"
	require.NoError(t, os.WriteFile(sourcePath, []byte(dql), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(routesDir, "auth", "auth.yaml"), []byte("Resource:\n  Types: ["), 0o644))

	result := &plan.Result{
		States: []*plan.State{
			{Parameter: state.Parameter{Name: "Auth1", In: &state.Location{Kind: state.KindComponent, Name: "../acl/auth"}}},
			{Parameter: state.Parameter{Name: "Auth2", In: &state.Location{Kind: state.KindComponent, Name: "../acl/auth"}}},
		},
	}
	diags := appendComponentTypes(&shape.Source{Path: sourcePath, DQL: dql}, result)
	require.NotEmpty(t, diags)
	invalidCount := 0
	for _, item := range diags {
		if item != nil && item.Code == dqldiag.CodeCompRouteInvalid {
			invalidCount++
		}
	}
	assert.Equal(t, 1, invalidCount)
}
