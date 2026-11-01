package cubecompose

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestGroupedSemanticPrefixPagesAfterAggregation(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE facts(tenant INTEGER,id INTEGER,amount REAL)",
		"INSERT INTO facts VALUES(1,1,60),(1,1,60),(1,2,110),(1,3,100),(1,4,50),(1,4,50),(1,5,90),(1,6,80),(1,7,-20),(1,8,NULL),(2,6,99999)"))
	catalog, err := NewCatalog(Field{Name: "id", Type: reflect.TypeFor[int64](), Role: Dimension}, Field{Name: "amount", Type: reflect.TypeFor[float64](), Role: Measure})
	require.NoError(t, err)
	prefix := 5
	plan, err := CompileWithResultLimit("SELECT t1.id AS entity,SUM(t1.amount) AS total FROM $CubeSQL1 AS t1 GROUP BY t1.id HAVING SUM(t1.amount)>=0 ORDER BY total DESC,entity ASC LIMIT 5", catalog, 1, 100, &prefix)
	require.NoError(t, err)
	type row struct {
		Entity int64   `sqlx:"entity"`
		Total  float64 `sqlx:"total"`
	}
	frames := []Frame{{SQL: "SELECT id,amount FROM facts WHERE tenant=?", Args: []any{1}}}
	for _, tc := range []struct {
		name string
		page Page
		want []row
	}{
		{"wide export cannot expand top five", Page{Limit: 25}, []row{{1, 120}, {2, 110}, {3, 100}, {4, 100}, {5, 90}}},
		{"lookahead inside prefix", Page{Limit: 2}, []row{{1, 120}, {2, 110}, {3, 100}}},
		{"middle with tie breaker", Page{Limit: 2, Offset: 2}, []row{{3, 100}, {4, 100}, {5, 90}}},
		{"last page without sixth row", Page{Limit: 2, Offset: 4}, []row{{5, 90}}},
		{"past prefix", Page{Limit: 2, Offset: 5}, []row{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paged, err := plan.WithPage(tc.page)
			require.NoError(t, err)
			query, args, err := paged.RenderPage(frames)
			require.NoError(t, err)
			require.Equal(t, []any{1, int64(0)}, args)
			db.AssertQuery(t, ctx, sqlite.Query{SQL: query, Args: args}, tc.want)
		})
	}
	require.Equal(t, 5, plan.Limit, "transport plans must not mutate authored plan")
	require.Zero(t, plan.Offset())
}

func TestGroupedSemanticPrefixTypedRankingAndHaving(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE items(tenant INTEGER,id INTEGER,label TEXT,cost REAL,units REAL,baseline REAL)",
		"INSERT INTO items VALUES(1,1,'Same',60,1,0),(1,1,'Same',60,1,0),(1,2,'Same',110,8,1),(1,3,'Third',100,7,1),(1,4,'Fourth',90,6,1),(1,5,'Fifth',80,5,1),(2,5,'Poison',99999,99999,0)"))
	catalog, err := NewCatalog(
		Field{Name: "id", Type: reflect.TypeFor[int64](), Role: Dimension},
		Field{Name: "label", Type: reflect.TypeFor[string](), Role: Dimension},
		Field{Name: "cost", Type: reflect.TypeFor[float64](), Role: Measure},
		Field{Name: "units", Type: reflect.TypeFor[float64](), Role: Measure},
		Field{Name: "baseline", Type: reflect.TypeFor[float64](), Role: Measure})
	require.NoError(t, err)
	values, err := NewValuesContract(reflect.TypeFor[struct {
		Mode    int `json:"mode"`
		Minimum int `json:"minimum"`
	}]())
	require.NoError(t, err)
	prefix := 3
	plan, err := CompileWithValues("SELECT t1.id AS entity,t1.label AS label,CASE WHEN $Values.mode=1 THEN SUM(t1.cost) ELSE SUM(t1.units)-SUM(t1.baseline) END AS value FROM $CubeSQL1 AS t1 GROUP BY t1.id,t1.label HAVING SUM(t1.cost)>=$Values.minimum ORDER BY value DESC,entity ASC LIMIT 5", catalog, 1, 100, &prefix, values)
	require.NoError(t, err)
	type row struct {
		Entity int64   `sqlx:"entity"`
		Label  string  `sqlx:"label"`
		Value  float64 `sqlx:"value"`
	}
	for _, tc := range []struct {
		values string
		offset int
		want   []row
	}{
		{`{"mode":1,"minimum":90}`, 0, []row{{1, "Same", 120}, {2, "Same", 110}, {3, "Third", 100}}},
		{`{"mode":2,"minimum":90}`, 0, []row{{2, "Same", 7}, {3, "Third", 6}, {4, "Fourth", 5}}},
		{`{"mode":2,"minimum":90}`, 2, []row{{4, "Fourth", 5}}},
		{`{"mode":1,"minimum":200}`, 0, []row{}},
	} {
		bound, err := plan.BindValues(ctx, json.RawMessage(tc.values))
		require.NoError(t, err)
		paged, err := bound.WithPage(Page{Limit: 25, Offset: tc.offset})
		require.NoError(t, err)
		query, args, err := paged.RenderPage([]Frame{{SQL: "SELECT id,label,cost,units,baseline FROM items WHERE tenant=?", Args: []any{1}}})
		require.NoError(t, err)
		db.AssertQuery(t, ctx, sqlite.Query{SQL: query, Args: args}, tc.want)
	}
}
