package cubecompose

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const hierarchySQL = "SELECT 'root' AS rowKey, NULL AS parentKey, 'All' AS category, t1.total_spend AS amount FROM $CubeSQL1 AS t1 UNION ALL SELECT t2.category AS rowKey, 'root' AS parentKey, t2.category AS category, t2.total_spend AS amount FROM $CubeSQL2 AS t2 ORDER BY rowKey LIMIT 2 OFFSET 1"

func unionCatalog(t *testing.T) *Catalog {
	c, e := NewCatalog(Field{Name: "category", Type: reflect.TypeFor[string](), Role: Dimension}, Field{Name: "total_spend", Type: reflect.TypeFor[float64](), Role: Measure})
	require.NoError(t, e)
	return c
}
func TestUnionRootLeafContractsAndBindings(t *testing.T) {
	p, e := Compile(hierarchySQL, unionCatalog(t), 2, 10)
	require.NoError(t, e)
	require.Equal(t, [][]string{{"total_spend"}, {"category", "total_spend"}}, p.Fields)
	require.Equal(t, reflect.TypeFor[*string](), p.Columns[1].RuntimeType())
	sql, args, e := p.Render([]Frame{{SQL: "SELECT SUM(amount) AS total_spend FROM facts WHERE tenant=?", Args: []any{7}}, {SQL: "SELECT 'leaf:' || category AS category,SUM(amount) AS total_spend FROM facts WHERE tenant=? GROUP BY category", Args: []any{7}}})
	require.NoError(t, e)
	require.Equal(t, []any{"root", "All", 7, "root", 7}, args)
	require.Equal(t, 1, strings.Count(sql, "LIMIT"))
	require.True(t, strings.HasSuffix(sql, "LIMIT 2 OFFSET 1"))
}
func TestUnionRejectsUnsupportedAndAmbiguousContracts(t *testing.T) {
	for _, sql := range []string{
		strings.Replace(hierarchySQL, "UNION ALL", "UNION", 1),
		strings.Replace(hierarchySQL, "UNION ALL", "WHERE t1.total_spend>0 UNION ALL", 1),
		strings.Replace(hierarchySQL, "t2.total_spend AS amount", "t2.total_spend AS parentKey", 1),
		strings.Replace(hierarchySQL, "t2.total_spend AS amount", "1 AS amount", 1),
		strings.Replace(hierarchySQL, "t2.total_spend AS amount", "t1.total_spend AS amount", 1),
		strings.Replace(hierarchySQL, "t2.total_spend AS amount", "t2.category AS amount", 1),
		strings.Replace(hierarchySQL, "t2.total_spend AS amount", "SUM(t2.total_spend) AS amount", 1),
		strings.Replace(hierarchySQL, "t2.total_spend AS amount", "t2.total_spend AS other", 1),
		strings.Replace(hierarchySQL, "FROM $CubeSQL2 AS t2", "FROM secrets AS t2", 1),
		strings.Replace(hierarchySQL, "UNION ALL", "LIMIT 1 UNION ALL", 1),
		strings.Replace(hierarchySQL, "ORDER BY rowKey", "GROUP BY rowKey ORDER BY rowKey", 1),
		strings.Replace(hierarchySQL, "ORDER BY rowKey", "WHERE t2.category='secret' ORDER BY rowKey", 1),
		strings.Replace(hierarchySQL, "ORDER BY rowKey", "ORDER BY t2.category", 1),
		strings.Replace(hierarchySQL, "LIMIT 2 OFFSET 1", "LIMIT 10 OFFSET 1", 1),
		strings.Replace(hierarchySQL, "ORDER BY rowKey LIMIT 2 OFFSET 1", "LIMIT 2 OFFSET 1", 1),
		strings.Replace(hierarchySQL, "'root' AS parentKey", "NULL AS parentKey", 1),
		hierarchySQL + "; DELETE FROM facts",
	} {
		t.Run(sql, func(t *testing.T) { _, e := Compile(sql, unionCatalog(t), 2, 10); require.Error(t, e) })
	}
}
