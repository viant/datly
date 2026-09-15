package reader_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	xstate "github.com/viant/xdatly/state"
)

func TestPhysicalProjectionPrunesOptionalRelationSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, `CREATE TABLE parents(id INTEGER,b_id INTEGER)`, `INSERT INTO parents VALUES(1,2)`))
	// The child table deliberately does not exist. An unselected typed relation
	// must neither force its parent's key into the result nor execute a child read.
	component := &spec.Component{RootView: &spec.View{Name: "parents", Selector: &spec.Selector{AllowFields: true}, Source: &spec.ViewSource{SQL: `SELECT b_id FROM parents`}}}
	typ := reflect.TypeFor[[]*declaredRoot]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
	require.NoError(t, err)
	for _, selected := range []bool{false, true, false} {
		fields := []string{"b_id"}
		if selected {
			fields = append(fields, "Children")
		}
		result, err := execution.ReadResult(ctx, &struct{}{}, sourceGuardBinder{xstate.Selectors{&xstate.NamedSelector{Name: "parents", Selector: xstate.Selector{Fields: fields}}}}, nil)
		if selected {
			require.ErrorContains(t, err, "not found column id")
			continue
		}
		require.NoError(t, err)
		rows := result.Data.([]*declaredRoot)
		require.Len(t, rows, 1)
		require.Equal(t, 2, rows[0].B_ID)
		require.Empty(t, rows[0].Children)
		evidence, err := result.Projection.Row(0)
		require.NoError(t, err)
		require.True(t, evidence.Fields().Has("B_ID"))
		require.False(t, evidence.Fields().Has("ID"))
	}
	require.Equal(t, `SELECT b_id FROM parents`, component.RootView.Source.SQL)
}

func TestNamedRelationAliasesAndDeferredPredicatesSQLite(t *testing.T) {
	for _, inner := range []string{`SELECT parent_id AS ParentKey,name FROM children`, `WITH RECURSIVE keys(ParentKey,name) AS (VALUES(1,'first'),(2,'second')) SELECT ParentKey,name FROM keys`} {
		for _, marker := range []string{`$WHERE_CRITERIA`, `$View.ParentJoinOn("WHERE","c.ParentKey")`} {
			t.Run(inner+"/"+marker, func(t *testing.T) {
				ctx := context.Background()
				h := sqlite.New(t)
				require.NoError(t, h.ExecStatements(ctx, `CREATE TABLE parents(id INTEGER,name TEXT)`, `CREATE TABLE children(parent_id INTEGER,name TEXT)`, `INSERT INTO parents VALUES(1,'one'),(2,'two')`, `INSERT INTO children VALUES(1,'first'),(2,'second')`))
				policy := &spec.Selector{AllowFields: true}
				childSQL := `SELECT c.ParentKey AS parent_id,c.name FROM (` + inner + `) c ` + marker
				child := &spec.View{Name: "children", Namespace: "c", Selector: policy, Source: &spec.ViewSource{SQL: childSQL}}
				root := &spec.View{Name: "parents", Selector: policy, Source: &spec.ViewSource{SQL: `SELECT id,name FROM parents $WHERE_CRITERIA`}, Relations: []*spec.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, View: child, On: []*spec.RelationLink{{ParentColumn: "id", ChildNamespace: "c", ChildColumn: "parent_id"}}}}}
				component := &spec.Component{RootView: root}
				typ := reflect.TypeFor[[]*sourceGuardRow]()
				plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
				require.NoError(t, err)
				execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
				require.NoError(t, err)
				selectors := xstate.Selectors{&xstate.NamedSelector{Name: "parents", Selector: xstate.Selector{Fields: []string{"id", "Children"}}}, &xstate.NamedSelector{Name: "children", Selector: xstate.Selector{Fields: []string{"name"}}}}
				result, err := execution.ReadResult(ctx, &struct{}{}, sourceGuardBinder{selectors}, nil)
				require.NoError(t, err)
				rows := result.Data.([]*sourceGuardRow)
				require.Len(t, rows, 2)
				for _, row := range rows {
					require.Len(t, row.Children, 1)
					require.Equal(t, row.ID, row.Children[0].ParentID)
				}
				evidence, err := result.Projection.Row(0)
				require.NoError(t, err)
				childEvidence, err := evidence.RelationRow("Children", 0)
				require.NoError(t, err)
				require.True(t, childEvidence.Fields().Has("ParentID"))
				require.True(t, childEvidence.Fields().Has("Name"))
				require.False(t, childEvidence.Fields().Has("ID"))
				require.Equal(t, childSQL, child.Source.SQL)
			})
		}
	}
}

func TestDeferredChildSQLSelectionSQLite(t *testing.T) {
	for _, tc := range []struct{ SQL, wantError string }{
		{`SELECT FROM children`, "source projection is unresolved"},
		{`SELECT parent_id,name FROM absent_children`, "no such table"},
		{`SELECT c.parent_id,c.name FROM (WITH RECURSIVE keys(name) AS (VALUES('child')) SELECT name FROM keys) c`, "no such column"},
	} {
		t.Run(tc.SQL, func(t *testing.T) {
			SQL := tc.SQL
			ctx := context.Background()
			h := sqlite.New(t)
			require.NoError(t, h.ExecStatements(ctx, `CREATE TABLE parents(id INTEGER,name TEXT)`, `INSERT INTO parents VALUES(1,'parent')`))
			child := &spec.View{Name: "children", Source: &spec.ViewSource{SQL: SQL}}
			root := &spec.View{Name: "parents", Selector: &spec.Selector{AllowFields: true}, Source: &spec.ViewSource{SQL: `SELECT id,name FROM parents`}, Relations: []*spec.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, View: child, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}}}}
			component := &spec.Component{RootView: root}
			typ := reflect.TypeFor[[]*sourceGuardRow]()
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
			require.NoError(t, err)
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
			require.NoError(t, err)
			for _, selected := range []bool{false, true, false} {
				fields := []string{"name"}
				if selected {
					fields = append(fields, "Children")
				}
				selectors := xstate.Selectors{&xstate.NamedSelector{Name: "parents", Selector: xstate.Selector{Fields: fields}}}
				result, err := execution.ReadResult(ctx, &struct{}{}, sourceGuardBinder{selectors}, nil)
				if selected {
					require.ErrorContains(t, err, tc.wantError)
					t.Logf("selected child error: %v", err)
					continue
				}
				require.NoError(t, err)
				rows := result.Data.([]*sourceGuardRow)
				require.Len(t, rows, 1)
				require.Equal(t, "parent", rows[0].Name)
				require.Zero(t, rows[0].ID)
				require.Empty(t, rows[0].Children)
			}
			require.Equal(t, SQL, child.Source.SQL)
		})
	}
}
