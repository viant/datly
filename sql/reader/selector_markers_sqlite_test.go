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

func TestReaderWildcardDeferredMarkersSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER,name TEXT,extra INTEGER,secret TEXT)", "CREATE TABLE children(id INTEGER,parent_id INTEGER,name TEXT,secret TEXT)", "INSERT INTO parents VALUES(1,'one',0,''),(2,'two',0,'')", "INSERT INTO children VALUES(10,1,'first',''),(20,2,'second','')"))
	for _, childSQL := range []string{
		"SELECT * FROM children $WHERE_CRITERIA ORDER BY id",
		`SELECT * FROM children $View.ParentJoinOn("WHERE","parent_id") ORDER BY id`,
	} {
		for _, selected := range []bool{false, true} {
			t.Run(childSQL+map[bool]string{false: "/none", true: "/selected"}[selected], func(t *testing.T) {
				policy := &spec.Selector{AllowFields: true, AllowOrderBy: true}
				component := &spec.Component{RootView: &spec.View{Name: "parents", Selector: policy, Source: &spec.ViewSource{SQL: "SELECT * FROM parents $WHERE_CRITERIA ORDER BY id"}}}
				output := reflect.TypeFor[[]*sourceGuardRow]()
				plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, DirectViewType: output})
				require.NoError(t, err)
				child := plan.Root.View.Relations[0].Of.View
				child.Spec.Source = &spec.ViewSource{SQL: childSQL}
				child.Spec.Selector = policy
				execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}})
				require.NoError(t, err)
				var selectors xstate.Selectors
				if selected {
					selectors = xstate.Selectors{&xstate.NamedSelector{Name: "parents", Selector: xstate.Selector{Fields: []string{"id", "Children"}, OrderBy: "id ASC"}}, &xstate.NamedSelector{Name: "children", Selector: xstate.Selector{Fields: []string{"name"}, OrderBy: "name ASC"}}}
				}
				result, err := execution.ReadResult(ctx, &struct{}{}, sourceGuardBinder{selectors}, nil)
				require.NoError(t, err)
				rows := result.Data.([]*sourceGuardRow)
				require.Len(t, rows, 2)
				for i, row := range rows {
					require.Equal(t, i+1, row.ID)
					require.Len(t, row.Children, 1)
					require.Equal(t, i+1, row.Children[0].ParentID)
				}
				root, err := result.Projection.Row(0)
				require.NoError(t, err)
				require.True(t, root.Fields().Known())
				require.True(t, root.Fields().Has("ID"))
				evidence, err := root.RelationRow("Children", 0)
				require.NoError(t, err)
				require.True(t, evidence.Fields().Known())
				require.True(t, evidence.Fields().Has("Name"))
				require.True(t, evidence.Fields().Has("ParentID"))
				require.Equal(t, !selected, evidence.Fields().Has("ID"))
			})
		}
	}
}
