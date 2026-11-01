package reader_test

import (
	"context"
	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx/testutil/sqlfault"
	xstate "github.com/viant/xdatly/state"
	"reflect"
	"testing"
	"testing/fstest"
)

// Neutral executable audit: ordinary aggregate projection retains the graph,
// while prepared wrapper projection deliberately remains scalar-only.
func TestProjectionGraphBoundaryAuditSQLite(t *testing.T) {
	type label struct {
		ID   int    `sqlx:"id"`
		Name string `sqlx:"name"`
	}
	type row struct {
		ID    int     `sqlx:"id"`
		Part  int     `sqlx:"part"`
		Ratio float64 `sqlx:"ratio"`
		Label *label  `view:"labels" on:"ID:id=ID:id" sql:"SELECT id,name FROM labels WHERE $COLUMN_IN"`
	}
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE facts(id INTEGER,part INTEGER,numerator REAL,denominator REAL,tenant TEXT)",
		"INSERT INTO facts VALUES(1,1,10,10,'allowed'),(1,2,0,90,'allowed'),(2,1,3,10,'allowed'),(3,1,99,1,'denied')",
		"CREATE TABLE labels(id INTEGER,name TEXT)", "INSERT INTO labels VALUES(1,'first'),(2,'second'),(3,'secret')"))
	grouped := true
	component := &spec.Component{RootView: &spec.View{Name: "facts", Groupable: &grouped, Selector: &spec.Selector{AllowFields: true, AllowOrderBy: true, AllowLimit: true, AllowOffset: true}, Source: &spec.ViewSource{SQL: "SELECT id,part,CASE WHEN SUM(denominator)=0 THEN NULL ELSE SUM(numerator)/SUM(denominator) END AS ratio FROM facts WHERE tenant='allowed' GROUP BY id,part"}}}
	typ := reflect.TypeFor[[]*row]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	binder := sourceGuardBinder{xstate.Selectors{&xstate.NamedSelector{Name: "facts", Selector: xstate.Selector{Fields: []string{"id", "ratio", "Label"}, OrderBy: "ratio DESC,id ASC", Limit: 1}}}}
	result, err := execution.ReadResult(ctx, &struct{}{}, binder, nil)
	require.NoError(t, err)
	rows := result.Data.([]*row)
	require.Len(t, rows, 1)
	require.Equal(t, 2, rows[0].ID)
	require.InDelta(t, 0.3, rows[0].Ratio, 0.00001)
	require.Equal(t, "second", rows[0].Label.Name)
	allBinder := sourceGuardBinder{xstate.Selectors{&xstate.NamedSelector{Name: "facts", Selector: xstate.Selector{Fields: []string{"id", "ratio", "Label"}, OrderBy: "ratio DESC,id ASC", Limit: 10}}}}
	all, err := execution.ReadResult(ctx, &struct{}{}, allBinder, nil)
	require.NoError(t, err)
	allRows := all.Data.([]*row)
	require.Len(t, allRows, 2)
	require.Equal(t, 1, allRows[1].ID)
	require.InDelta(t, 0.1, allRows[1].Ratio, 0.00001)
	require.Equal(t, "first", allRows[1].Label.Name)
	prepared, err := execution.PrepareQuery(ctx, &struct{}{}, binder, nil)
	require.NoError(t, err)
	projected, err := prepared.Projection.ReadProjection(ctx, dexec.ProjectionRequest{SQL: "SELECT id,ratio FROM (" + prepared.SQL + ") q ORDER BY ratio DESC,id ASC LIMIT 1", Args: prepared.Args, RowType: reflect.TypeFor[row]()})
	require.NoError(t, err)
	projectedRows := projected.([]*row)
	require.Len(t, projectedRows, 1)
	require.Equal(t, 2, projectedRows[0].ID)
	require.InDelta(t, 0.3, projectedRows[0].Ratio, 0.00001)
	require.Nil(t, projectedRows[0].Label)
	type aliasRow struct {
		Key   int     `sqlx:"key" json:"key"`
		Ratio float64 `sqlx:"ratio" json:"ratio"`
	}
	graph, err := prepared.Projection.(dexec.ProjectionGraphReader).PrepareProjectionGraph(ctx, dexec.ProjectionGraphRequest{RowType: reflect.TypeFor[aliasRow](), Relations: []dexec.ProjectionRelation{{Name: "Label", Output: "label", Keys: []dexec.ProjectionKey{{Source: "id", Output: "key"}}}}})
	require.NoError(t, err)
	projected, err = prepared.Projection.ReadProjection(ctx, dexec.ProjectionRequest{SQL: "SELECT id AS key,ratio FROM (" + prepared.SQL + ") q ORDER BY ratio DESC,id ASC LIMIT 1", Args: prepared.Args, RowType: graph.RowType()})
	require.NoError(t, err)
	loaded, err := graph.Load(ctx, projected)
	require.NoError(t, err)
	labelValue := reflect.ValueOf(loaded).Index(0).Elem().FieldByName("Label").Interface().(*label)
	require.Equal(t, "second", labelValue.Name)
	require.NoError(t, db.ExecStatements(ctx, "INSERT INTO labels VALUES(2,'duplicate')"))
	_, err = graph.Load(ctx, projected)
	require.ErrorContains(t, err, "duplicate keys")
}

func TestBoundedRankingSubviewGraphAuditSQLite(t *testing.T) {
	type label struct {
		ID   int    `sqlx:"id"`
		Name string `sqlx:"name"`
	}
	type row struct {
		ID    int     `sqlx:"id"`
		Ratio float64 `sqlx:"ratio"`
		Label *label  `view:"labels" on:"ID:id=ID:id" sql:"SELECT id,name FROM labels WHERE $COLUMN_IN"`
	}
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE facts(id INTEGER,part INTEGER,numerator REAL,denominator REAL,tenant TEXT)",
		"INSERT INTO facts VALUES(1,1,10,10,'allowed'),(1,2,0,90,'allowed'),(2,1,2,10,'allowed'),(3,1,3,10,'allowed'),(4,1,4,10,'allowed'),(5,1,5,10,'allowed'),(6,1,6,10,'allowed'),(7,1,7,10,'allowed'),(8,1,99,1,'denied')",
		"CREATE TABLE labels(id INTEGER,name TEXT)", "INSERT INTO labels VALUES(1,'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five'),(6,'six'),(7,'seven'),(8,'secret')"))
	grouped := false
	connection := db.FaultDB(t, func(ctx context.Context, call sqlfault.Call) error {
		if call.Phase == "query" {
			t.Log(call.SQL)
		}
		return nil
	})
	resources := fstest.MapFS{
		"sql/ranking.sql":         {Data: []byte("SELECT ranked.id,ranked.ratio FROM (SELECT id,${embed:metrics.sql} ${embed:authorized_fact.sql} GROUP BY id ORDER BY ratio DESC,id ASC LIMIT 5) ranked")},
		"sql/metrics.sql":         {Data: []byte("CASE WHEN SUM(denominator)=0 THEN NULL ELSE SUM(numerator)/SUM(denominator) END AS ratio")},
		"sql/authorized_fact.sql": {Data: []byte("FROM facts WHERE tenant='allowed'")},
		"sql/cube.sql":            {Data: []byte("SELECT id,part,${embed:metrics.sql} ${embed:authorized_fact.sql} GROUP BY id,part")},
	}
	// Both entries share the authored formula and fact/predicate fragments. The
	// ranking entry owns its intentional aggregate grain and semantic prefix.
	component := &spec.Component{RootView: &spec.View{Name: "facts", Groupable: &grouped, Selector: &spec.Selector{AllowFields: true, AllowOrderBy: true, AllowLimit: true, AllowOffset: true}, Source: &spec.ViewSource{URI: "sql/ranking.sql"}}}
	typ := reflect.TypeFor[[]*row]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ, Resources: resources})
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: connection}})
	require.NoError(t, err)
	for _, tc := range []struct {
		order         string
		limit, offset int
		ids           []int
	}{
		{"id ASC", 100, 0, []int{3, 4, 5, 6, 7}},
		{"id ASC", 2, 1, []int{4, 5}},
		{"ratio ASC,id ASC", 2, 0, []int{3, 4}},
		{"id DESC", 2, 4, []int{3}},
		{"id ASC", 2, 5, nil},
	} {
		binder := sourceGuardBinder{xstate.Selectors{&xstate.NamedSelector{Name: "facts", Selector: xstate.Selector{Fields: []string{"id", "ratio", "Label"}, OrderBy: tc.order, Limit: tc.limit, Offset: tc.offset}}}}
		result, err := execution.ReadResult(ctx, &struct{}{}, binder, nil)
		require.NoError(t, err)
		rows := result.Data.([]*row)
		require.Len(t, rows, len(tc.ids))
		for i, row := range rows {
			require.Equal(t, tc.ids[i], row.ID)
			require.InDelta(t, float64(row.ID)/10, row.Ratio, 0.00001)
			require.NotNil(t, row.Label)
			require.Equal(t, row.ID, row.Label.ID)
			require.NotEmpty(t, row.Label.Name)
		}
	}
	require.Empty(t, component.RootView.Source.SQL)
	require.Equal(t, "sql/ranking.sql", component.RootView.Source.URI)
}
