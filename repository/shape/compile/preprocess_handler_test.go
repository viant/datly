package compile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/compile/pipeline"
	dqlpre "github.com/viant/datly/repository/shape/dql/preprocess"
	dqlstmt "github.com/viant/datly/repository/shape/dql/statement"
)

func TestIsHandlerSignal(t *testing.T) {
	assert.True(t, isHandlerSignal(&shape.Source{DQL: `/* {"Type":"campaign/patch.Handler"} */`}))
	assert.True(t, isHandlerSignal(&shape.Source{DQL: `$Nop($Data)`}))
	assert.True(t, isHandlerSignal(&shape.Source{DQL: `$Proxy($Data)`}))
	assert.False(t, isHandlerSignal(&shape.Source{DQL: `SELECT id FROM proxy_audit`}))
	assert.False(t, isHandlerSignal(&shape.Source{DQL: `/* proxy disabled */ SELECT 1`}))
	assert.False(t, isHandlerSignal(&shape.Source{DQL: `SELECT 1`}))
}

func TestBuildHandlerFromContractIfNeeded_LegacyFallbackViews(t *testing.T) {
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "dql", "platform", "campaign", "post.dql")
	require.NoError(t, os.MkdirAll(filepath.Dir(sourcePath), 0o755))
	dql := `/* {"Type":"campaign/patch.Handler","Connector":"ci_ads"} */`
	require.NoError(t, os.WriteFile(sourcePath, []byte(dql), 0o644))

	routeDir := filepath.Join(tempDir, "repo", "dev", "Datly", "routes", "platform", "campaign", "patch", "post")
	require.NoError(t, os.MkdirAll(routeDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(routeDir, "post.sql"), []byte(`SELECT 1`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(routeDir, "CurCampaign.sql"), []byte(`SELECT * FROM CI_CAMPAIGN`), 0o644))

	source := &shape.Source{Path: sourcePath, DQL: dql}
	pre := dqlpre.Prepare(source.DQL)
	statements := dqlstmt.New(pre.SQL)
	decision := pipeline.Classify(statements)
	result := &handlerPreprocessResult{Pre: pre, Statements: statements, Decision: decision, EffectiveSource: source}
	applied := buildHandlerFromContractIfNeeded(result, source, defaultCompilePathLayout())
	require.True(t, applied)
	require.NotNil(t, result)
	require.NotEmpty(t, result.LegacyViews)
	assert.Equal(t, "post", result.LegacyViews[0].Name)
}

func TestBuildGeneratedFallbackIfNeeded_GeneratedCompanion(t *testing.T) {
	tempDir := t.TempDir()
	dqlPath := filepath.Join(tempDir, "platform", "adorder", "patch.dql")
	require.NoError(t, os.MkdirAll(filepath.Join(filepath.Dir(dqlPath), "gen", "adorder"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(dqlPath), "gen", "adorder", "patch.dql"), []byte("SELECT o.id FROM ORDERS o"), 0o644))
	source := &shape.Source{
		Name: "patch",
		Path: dqlPath,
		DQL:  `/* {"Type":"adorder/patch.Handler"} */`,
	}
	pre := dqlpre.Prepare(source.DQL)
	statements := dqlstmt.New(pre.SQL)
	decision := pipeline.Classify(statements)
	result := &handlerPreprocessResult{Pre: pre, Statements: statements, Decision: decision, EffectiveSource: source}
	applied := buildGeneratedFallbackIfNeeded(result, source, defaultCompilePathLayout())
	require.True(t, applied)
	require.NotNil(t, result)
	assert.Empty(t, result.LegacyViews)
	assert.Contains(t, result.Pre.SQL, "SELECT o.id FROM ORDERS o")
	assert.True(t, result.Decision.HasRead)
}

func TestResolveGeneratedLegacySource(t *testing.T) {
	tempDir := t.TempDir()
	genPath := filepath.Join(tempDir, "dql", "system", "session", "gen", "session", "patch.dql")
	require.NoError(t, os.MkdirAll(filepath.Dir(genPath), 0o755))
	require.NoError(t, os.WriteFile(genPath, []byte(`/* {"Method":"PATCH","URI":"/v1/api/system/session"} */`), 0o644))
	legacySQL := filepath.Join(tempDir, "dql", "system", "session", "patch.sql")
	require.NoError(t, os.MkdirAll(filepath.Dir(legacySQL), 0o755))
	require.NoError(t, os.WriteFile(legacySQL, []byte(`/* {"Type":"session/patch.Handler"} */`), 0o644))

	source := &shape.Source{Path: genPath, DQL: `/* {"Method":"PATCH","URI":"/v1/api/system/session"} */`}
	actual := resolveGeneratedLegacySource(source)
	require.NotNil(t, actual)
	assert.Equal(t, legacySQL, actual.Path)
	assert.Contains(t, actual.DQL, `"Type":"session/patch.Handler"`)
}

func TestBuildGeneratedFallbackIfNeeded_GeneratedLegacyRoute(t *testing.T) {
	tempDir := t.TempDir()
	genPath := filepath.Join(tempDir, "dql", "system", "session", "gen", "session", "patch.dql")
	require.NoError(t, os.MkdirAll(filepath.Dir(genPath), 0o755))
	require.NoError(t, os.WriteFile(genPath, []byte(`/* {"Method":"PATCH","URI":"/v1/api/system/session"} */`), 0o644))
	legacySQL := filepath.Join(tempDir, "dql", "system", "session", "patch.sql")
	require.NoError(t, os.MkdirAll(filepath.Dir(legacySQL), 0o755))
	require.NoError(t, os.WriteFile(legacySQL, []byte(`/* {"Type":"session/patch.Handler","Connector":"system"} */`), 0o644))

	routesDir := filepath.Join(tempDir, "repo", "dev", "Datly", "routes", "system", "session", "patch")
	require.NoError(t, os.MkdirAll(routesDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(routesDir), "patch.yaml"), []byte(`Resource:
  Views:
    - Name: patch
      Mode: SQLExec
      Connector:
        Ref: system
      Template:
        SourceURL: patch/patch.sql
  Parameters:
    - Name: Session
      In:
        Kind: body
        Name: data
  Types:
    - Name: Input
      DataType: "*Input"
      Package: session/patch
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(routesDir, "patch.sql"), []byte(`$Nop($Unsafe.Session)`), 0o644))

	source := &shape.Source{Path: genPath, DQL: `/* {"Method":"PATCH","URI":"/v1/api/system/session"} */`}
	pre := dqlpre.Prepare(source.DQL)
	statements := dqlstmt.New(pre.SQL)
	decision := pipeline.Classify(statements)
	result := &handlerPreprocessResult{Pre: pre, Statements: statements, Decision: decision, EffectiveSource: source}
	applied := buildGeneratedFallbackIfNeeded(result, source, defaultCompilePathLayout())
	require.True(t, applied)
	require.NotNil(t, result)
	require.True(t, result.ForceLegacyContract)
	require.NotNil(t, result.EffectiveSource)
	assert.Equal(t, legacySQL, result.EffectiveSource.Path)
	require.NotEmpty(t, result.LegacyViews)
	assert.Equal(t, "patch", result.LegacyViews[0].Name)
}

func TestBuildLegacyRouteFallbackIfNeeded_NoLegacyRoute(t *testing.T) {
	source := &shape.Source{Path: filepath.Join(t.TempDir(), "dql", "x", "y", "z.dql"), DQL: `SELECT 1`}
	pre := dqlpre.Prepare(source.DQL)
	statements := dqlstmt.New(pre.SQL)
	decision := pipeline.Classify(statements)
	result := &handlerPreprocessResult{Pre: pre, Statements: statements, Decision: decision, EffectiveSource: source}
	applied := buildLegacyRouteFallbackIfNeeded(result, source, defaultCompilePathLayout())
	assert.False(t, applied)
	assert.Empty(t, result.LegacyViews)
	assert.False(t, result.ForceLegacyContract)
}
