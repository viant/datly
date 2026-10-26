package executionhint

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func option(value bool) *spec.ExecutionOptions {
	return &spec.ExecutionOptions{MySQL: &spec.MySQLExecutionOptions{ConditionFanoutFilter: &value}}
}

func TestRender(t *testing.T) {
	for _, tc := range []struct{ name, sql, contains, failure string }{
		{"plain", "SELECT id FROM audience ORDER BY id LIMIT ?", "SELECT /*+ SET_VAR(optimizer_switch='condition_fanout_filter=off') */ id", ""},
		{"cte", "WITH c AS (SELECT id FROM audience) SELECT id FROM c", ") SELECT /*+ SET_VAR", ""},
		{"comments and quoted select", "/* SELECT */ SELECT 'SELECT', `SELECT` FROM audience", "/* SELECT */ SELECT /*+ SET_VAR", ""},
		{"unrelated hint", "SELECT /*+ MAX_EXECUTION_TIME(1000) */ id FROM audience", "MAX_EXECUTION_TIME(1000)  SET_VAR", ""},
		{"merge", "SELECT /*+ SET_VAR(optimizer_switch='mrr=on') MAX_EXECUTION_TIME(1000) */ id FROM audience", "optimizer_switch='mrr=on,condition_fanout_filter=off'", ""},
		{"same", "SELECT /*+ SET_VAR(optimizer_switch='mrr=on,condition_fanout_filter=off') */ id FROM audience", "mrr=on,condition_fanout_filter=off", ""},
		{"conflict", "SELECT /*+ SET_VAR(optimizer_switch='condition_fanout_filter=on') */ id FROM audience", "", "conflicts"},
		{"default conflict", "SELECT /*+ SET_VAR(optimizer_switch='condition_fanout_filter=default') */ id FROM audience", "", "conflicts"},
		{"duplicate switch", "SELECT /*+ SET_VAR(optimizer_switch='mrr=on,mrr=off') */ id FROM audience", "", "duplicate"},
		{"duplicate variable", "SELECT /*+ SET_VAR(optimizer_switch='mrr=on') SET_VAR(optimizer_switch='mrr=on') */ id FROM audience", "", "unambiguous"},
		{"unsupported expression", "SELECT /*+ SET_VAR(optimizer_switch=concat('mrr=on')) */ id FROM audience", "", "unambiguous"},
		{"multiple comments", "SELECT /*+ MAX_EXECUTION_TIME(1000) */ /*+ SET_VAR(optimizer_switch='mrr=on') */ id FROM audience", "", "multiple"},
		{"incomplete quote", "SELECT 'bad", "", "unterminated"},
		{"multiple statements", "SELECT id FROM audience; SELECT 1", "", "multiple"},
		{"trailing comment", "SELECT id FROM audience; /* trailing */", "SELECT /*+ SET_VAR", ""},
		{"qualified switch", "SELECT /*+ SET_VAR(@@optimizer_switch='mrr=on') */ id FROM audience", "", "qualified"},
		{"quoted switch", "SELECT /*+ SET_VAR(`optimizer_switch`='mrr=on') */ id FROM audience", "", "quoted optimizer_switch"},
		{"mode-dependent quote", "SELECT /*+ SET_VAR(optimizer_switch=\"mrr=on\") */ id FROM audience", "", "single-quoted"},
		{"not select", "/* comment */ INSERT INTO a SELECT id FROM audience", "", "SELECT statement"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := Render(tc.sql, "MySQL", option(false))
			if tc.failure != "" {
				require.ErrorContains(t, err, tc.failure)
				return
			}
			require.NoError(t, err)
			require.Contains(t, actual, tc.contains)
			require.Equal(t, 1, strings.Count(strings.ToLower(actual), "condition_fanout_filter=off"))
			again, err := Render(actual, "mysql", option(false))
			require.NoError(t, err)
			require.Equal(t, actual, again, "rendering must be idempotent")
			require.Equal(t, strings.Count(tc.sql, "?"), strings.Count(actual, "?"))
		})
	}
}

func TestInactiveAndInvalid(t *testing.T) {
	for _, product := range []string{"SQLite", "BigQuery", "PostgreSQL", "MariaDB", ""} {
		text, err := Render("SELECT id FROM audience", product, option(false))
		require.NoError(t, err)
		require.Equal(t, "SELECT id FROM audience", text)
		_, err = Render(text, product, option(true))
		require.ErrorContains(t, err, "only false")
	}
	text, err := Render("SELECT id FROM audience", "mysql", nil)
	require.NoError(t, err)
	require.Equal(t, "SELECT id FROM audience", text)
	copy := option(false).Clone()
	*copy.MySQL.ConditionFanoutFilter = true
	require.Error(t, copy.Validate())
}
