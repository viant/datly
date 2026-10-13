package command

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/cmd/options"
)

func TestParseShapeRulePath(t *testing.T) {
	method, uri := parseShapeRulePath(`/* {"Method":"POST","URI":"/v1/api/orders"} */ SELECT 1`, "orders", "/v1/api")
	assert.Equal(t, "POST", method)
	assert.Equal(t, "/v1/api/orders", uri)

	method, uri = parseShapeRulePath(`SELECT 1`, "orders", "/v1/api")
	assert.Equal(t, "GET", method)
	assert.Equal(t, "/v1/api/orders", uri)
}

func TestRoutePathForShape(t *testing.T) {
	rule := &options.Rule{Project: "/repo", Source: []string{"/repo/dql/platform/campaign/post.dql"}}
	routeYAML, routeRoot, relDir, stem, err := routePathForShape(rule, "/repo/dev", "/repo/dql/platform/campaign/post.dql")
	require.NoError(t, err)
	assert.Equal(t, "/repo/dev/Datly/routes/platform/campaign/post.yaml", routeYAML)
	assert.Equal(t, "/repo/dev/Datly/routes", routeRoot)
	assert.Equal(t, filepath.ToSlash("platform/campaign"), relDir)
	assert.Equal(t, "post", stem)
}

func TestTranslateShape_PreservesPredicateBuilderBlocksInGeneratedSQL(t *testing.T) {
	ctx := context.Background()
	projectDir := t.TempDir()
	repoDir := filepath.Join(projectDir, "repo", "dev")
	dqlDir := filepath.Join(projectDir, "dql", "opaque")
	sqlDir := filepath.Join(dqlDir, "sql")
	require.NoError(t, os.MkdirAll(sqlDir, 0o755))
	require.NoError(t, os.MkdirAll(repoDir, 0o755))

	sqlSource := `SELECT
    x0.k_a,
    x0.k_b,
    z9.m_q,
    SUM(x0.v_n) AS agg_alpha,
    AVG(z9.v_r) AS agg_beta
FROM
    data_alpha x0
LEFT JOIN data_beta z9
       ON x0.k_b = z9.k_b

${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}
GROUP BY
    x0.k_a,
    x0.k_b,
    z9.m_q
 ${predicate.Builder().CombineOr($predicate.FilterGroup(1, "HAVING")).Build("HAVING")}`
	require.NoError(t, os.WriteFile(filepath.Join(sqlDir, "opaque_source.sql"), []byte(sqlSource), 0o600))

	dqlSource := `/* {"URI":"/opaque/report","Name":"OpaqueReport"} */
#set($_ = $cube())
#set($_ = $Cutoff<string>(query/cutoff).Optional().WithPredicate(0, 'greater_or_equal', 'x0', 'k_a'))
#set($_ = $Threshold<int>(query/threshold).Optional().WithPredicate(1, 'expr', '(SUM(v_n) >= ?)'))

SELECT opaque_root.*,
       grouping_enabled(opaque_root),
       allow_nulls(opaque_root),
       set_limit(opaque_root, 25)
FROM (${embed:sql/opaque_source.sql}) opaque_root`
	dqlPath := filepath.Join(dqlDir, "opaque_report.dql")
	require.NoError(t, os.WriteFile(dqlPath, []byte(dqlSource), 0o600))

	opts := &options.Options{
		Translate: &options.Translate{
			Rule: options.Rule{
				Project: projectDir,
				Source:  []string{dqlPath},
				Engine:  options.EngineShape,
			},
			Repository: options.Repository{
				RepositoryURL: repoDir,
				APIPrefix:     "/v1/api",
			},
		},
	}
	require.NoError(t, opts.Init(ctx))

	svc := New()
	require.NoError(t, svc.translateShape(ctx, opts))

	generatedSQLPath := filepath.Join(repoDir, "Datly", "routes", "opaque", "opaque_report", "opaque_report.sql")
	data, err := os.ReadFile(generatedSQLPath)
	require.NoError(t, err)
	generated := string(data)
	assert.True(t, strings.Contains(generated, `${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}`))
	assert.True(t, strings.Contains(generated, `${predicate.Builder().CombineOr($predicate.FilterGroup(1, "HAVING")).Build("HAVING")}`))
}
