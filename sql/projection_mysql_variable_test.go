package sql

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMySQLVariableProjectionPreservesSQL(t *testing.T) {
	const source = "SELECT tokens.* FROM (SELECT (@i:=@i+1) AS POS, t.TARGET_KEY FROM (SELECT TARGET_KEY FROM records) t, (SELECT @i:=0) AS p) tokens"
	result, err := (SelectorProjection{SQL: source}).Prepare(nil)
	require.NoError(t, err)
	require.Equal(t, source, result.Render(result.Source))
	for _, name := range []string{"POS", "TARGET_KEY"} {
		found, complete, err := (SelectorProjection{SQL: source}).HasOutput(name)
		require.NoError(t, err)
		require.True(t, found)
		require.True(t, complete)
	}
	_, err = (SelectorProjection{SQL: source}).Prepare([]string{"missing"})
	require.Error(t, err)
	for _, sql := range []string{"SELECT (@i:=) AS POS", "SELECT (@i:=@i+) AS POS", "SELECT f(@) AS POS", "SELECT f(@i,) AS POS"} {
		_, err := parseProjectionQuery(sql)
		require.Error(t, err, sql)
	}
}
