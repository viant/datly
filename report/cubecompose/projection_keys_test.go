package cubecompose

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"reflect"
	"testing"
)

func TestProjectionKeysPreserveOnlyDirectGroupedDimensions(t *testing.T) {
	catalog, err := NewCatalog(Field{Name: "id", Type: reflect.TypeFor[int](), Role: Dimension}, Field{Name: "amount", Type: reflect.TypeFor[float64](), Role: Measure})
	require.NoError(t, err)
	for _, test := range []struct {
		sql  string
		want map[string]string
	}{
		{"SELECT t1.id AS entity,t1.amount AS value FROM $CubeSQL1 t1 LIMIT 5", map[string]string{"id": "entity"}},
		{"SELECT t1.id AS entity,CASE WHEN t1.amount>0 THEN t1.amount ELSE NULL END AS value FROM $CubeSQL1 t1 LIMIT 5", map[string]string{"id": "entity"}},
		{"SELECT t1.id AS entity,SUM(t1.amount) AS value FROM $CubeSQL1 t1 GROUP BY t1.id LIMIT 5", map[string]string{"id": "entity"}},
		{"SELECT t1.id AS entity,CASE WHEN SUM(t1.amount)>0 THEN SUM(t1.amount) ELSE NULL END AS value FROM $CubeSQL1 t1 LIMIT 5", map[string]string{}},
		{"SELECT t1.id+0 AS entity,t1.amount AS value FROM $CubeSQL1 t1 LIMIT 5", map[string]string{}},
	} {
		plan, err := Compile(test.sql, catalog, 1, 10)
		require.NoError(t, err)
		keys, err := plan.ProjectionKeys()
		require.NoError(t, err)
		require.Equal(t, test.want, keys, test.sql)
	}
}

func TestProjectionKeysRejectNestedAggregateProvenance(t *testing.T) {
	catalog, err := NewCatalog(Field{Name: "id", Type: reflect.TypeFor[int](), Role: Dimension}, Field{Name: "amount", Type: reflect.TypeFor[float64](), Role: Measure})
	require.NoError(t, err)
	for _, sql := range []string{
		"SELECT t1.id AS entity,CASE WHEN t1.amount BETWEEN SUM(t1.amount) AND 100 THEN 1 ELSE 0 END AS flag FROM $CubeSQL1 t1 LIMIT 5",
		"SELECT t1.id AS entity,CASE WHEN t1.amount IN (SUM(t1.amount),100) THEN 1 ELSE 0 END AS flag FROM $CubeSQL1 t1 LIMIT 5",
		"SELECT t1.id AS entity,CASE WHEN t1.amount>0 THEN CASE WHEN t1.amount IN (SUM(t1.amount),100) THEN 1 ELSE 0 END ELSE 0 END AS flag FROM $CubeSQL1 t1 LIMIT 5",
		"SELECT t1.id AS entity FROM $CubeSQL1 t1 WHERE t1.amount BETWEEN SUM(t1.amount) AND 100 LIMIT 5",
		"SELECT t1.id AS entity FROM $CubeSQL1 t1 HAVING MAX(t1.amount)>0 LIMIT 5",
		"SELECT t1.id AS entity FROM $CubeSQL1 t1 ORDER BY SUM(t1.amount) LIMIT 5",
	} {
		plan, err := Compile(sql, catalog, 1, 10)
		require.NoError(t, err, sql)
		keys, err := plan.ProjectionKeys()
		require.NoError(t, err, sql)
		require.Empty(t, keys, sql)
	}
	// Full native traversal must not reject the nonaggregate versions.
	for _, expression := range []string{
		"CASE WHEN t1.amount BETWEEN 1 AND 100 THEN 1 ELSE 0 END",
		"CASE WHEN t1.amount IN (1,100) THEN 1 ELSE 0 END",
		"CASE WHEN t1.amount>0 THEN CASE WHEN t1.amount BETWEEN 1 AND 100 THEN 1 ELSE 0 END ELSE 0 END",
	} {
		plan, err := Compile("SELECT t1.id AS entity,"+expression+" AS flag FROM $CubeSQL1 t1 LIMIT 5", catalog, 1, 10)
		require.NoError(t, err)
		keys, err := plan.ProjectionKeys()
		require.NoError(t, err)
		require.Equal(t, map[string]string{"id": "entity"}, keys)
	}
}

func TestProjectionKeysQualifiedGroupingAvoidsExecutedAliasCollision(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE facts(id INTEGER,amount REAL)", "INSERT INTO facts VALUES(1,10),(2,10)"))
	catalog, err := NewCatalog(Field{Name: "id", Type: reflect.TypeFor[int64](), Role: Dimension}, Field{Name: "amount", Type: reflect.TypeFor[float64](), Role: Measure})
	require.NoError(t, err)
	type row struct {
		ID    int64   `sqlx:"amount"`
		Total float64 `sqlx:"total"`
	}
	for _, test := range []struct {
		group  string
		reject bool
		want   []row
	}{
		{"amount", true, []row{{1, 20}}},
		{"t1.id", false, []row{{1, 10}, {2, 10}}},
	} {
		plan, err := Compile("SELECT t1.id AS amount,SUM(t1.amount) AS total FROM $CubeSQL1 t1 GROUP BY "+test.group+" ORDER BY t1.id LIMIT 5", catalog, 1, 10)
		require.NoError(t, err)
		sql, args, err := plan.Render([]Frame{{SQL: "SELECT id,amount FROM facts"}})
		require.NoError(t, err)
		db.AssertQuery(t, ctx, sqlite.Query{SQL: sql, Args: args}, test.want)
		keys, err := plan.ProjectionKeys()
		if test.reject {
			require.ErrorContains(t, err, "explicit qualified dimension GROUP BY")
			require.Nil(t, keys)
		} else {
			require.NoError(t, err)
			require.Equal(t, map[string]string{"id": "amount"}, keys)
		}
	}
	// Even apparently noncolliding aliases and positional/computed groupings are
	// outside the initial opt-in contract; source columns can be uncatalogued.
	for _, group := range []string{"entity", "1", "t1.id+0"} {
		plan, err := Compile("SELECT t1.id AS entity,SUM(t1.amount) AS total FROM $CubeSQL1 t1 GROUP BY "+group+" LIMIT 5", catalog, 1, 10)
		require.NoError(t, err)
		_, err = plan.ProjectionKeys()
		require.ErrorContains(t, err, "explicit qualified dimension GROUP BY")
	}
}
