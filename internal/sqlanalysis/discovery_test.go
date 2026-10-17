package sqlanalysis

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiscoveryCompositePredicateScopes(t *testing.T) {
	call := `$criteria.CompositeIn("r", $Unsafe.Keys($Rows, $Unsafe.Other($Current)))`
	for _, source := range []string{
		`SELECT r.id FROM records r WHERE ` + call,
		`SELECT r.id FROM records r WHERE r.id>0 AND (` + call + ` OR ` + call + `)`,
		`SELECT r.id,COUNT(*) AS total FROM records r GROUP BY r.id HAVING ` + call,
		`SELECT r.id FROM records r JOIN other o ON o.id=r.id AND ` + call,
		`SELECT CASE WHEN ` + call + ` THEN 1 ELSE 0 END AS selected FROM records r`,
		`WITH current AS (SELECT r.id FROM records r WHERE ` + call + `) SELECT id FROM current`,
		`SELECT x.id FROM (SELECT r.id FROM records r WHERE ` + call + `) x`,
	} {
		t.Run(source, func(t *testing.T) {
			got, err := DiscoveryPredicates(source)
			require.NoError(t, err)
			require.Equal(t, strings.ReplaceAll(source, call, "(1 = 0)"), got)
		})
	}
}

func TestDiscoveryCompositePredicateRejectsNonPredicateScopes(t *testing.T) {
	call := `$criteria.CompositeIn("r", $Unsafe.Keys($Rows))`
	for _, source := range []string{
		`SELECT ` + call + ` AS value FROM records r`,
		`SELECT r.id FROM ` + call + ` r`,
		`SELECT r.id FROM records r ORDER BY ` + call,
		`SELECT r.id FROM records r GROUP BY ` + call,
		`SELECT CASE ` + call + ` WHEN 1 THEN 1 ELSE 0 END AS value FROM records r`,
		`SELECT CASE WHEN r.id>0 THEN ` + call + ` ELSE 0 END AS value FROM records r`,
	} {
		_, err := DiscoveryPredicates(source)
		require.Error(t, err, source)
	}
}

func TestDiscoveryCompositePredicateSyntaxAndProtectedText(t *testing.T) {
	for _, call := range []string{
		`$criteria.CompositeIn("r")`,
		`$criteria.CompositeIn("r", $Rows, $Other)`,
		`$criteria.CompositeIn("r", $Rows).Other()`,
		`$criteria.CompositeIn("r", $Unsafe.Keys($Rows)`,
	} {
		_, err := DiscoveryPredicates(`SELECT id FROM records WHERE ` + call)
		require.Error(t, err, call)
	}
	protected := `SELECT '$criteria.CompositeIn("r", $Unsafe.Keys())' AS label FROM records /* $criteria.CompositeIn("r", $Rows) */ -- $criteria.CompositeIn("r", $Rows)`
	got, err := DiscoveryPredicates(protected)
	require.NoError(t, err)
	require.Equal(t, protected, got)
	unknown := `SELECT id FROM records WHERE $criteria.Other($Rows)`
	got, err = DiscoveryPredicates(unknown)
	require.NoError(t, err)
	require.Equal(t, unknown, got)
	_, err = SQL(`SELECT $Unsafe.Columns FROM records`, nil)
	require.Error(t, err, "ordinary static analysis must retain its authority checks")
}
