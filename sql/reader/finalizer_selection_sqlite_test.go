package reader_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	xstate "github.com/viant/xdatly/state"
)

func TestFinalizerSelectionDistinguishesExcludedFromEmptyRelationSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx,
		`CREATE TABLE parents(id INTEGER, name TEXT)`,
		`CREATE TABLE children(id INTEGER, parent_id INTEGER)`,
		`INSERT INTO parents VALUES(1,'empty')`))
	type child struct {
		ID       int `sqlx:"id"`
		ParentID int `sqlx:"parent_id"`
	}
	type parent struct {
		ID       int      `sqlx:"id"`
		Name     string   `sqlx:"name"`
		Children []*child `json:"entries"`
	}
	policy := &spec.Selector{AllowFields: true}
	component := &spec.Component{RootView: &spec.View{Name: "parents", Namespace: "parents", Selector: policy,
		Source: &spec.ViewSource{SQL: `SELECT parents.* FROM parents`},
		Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany,
			View: &spec.View{Name: "children", Namespace: "children", Source: &spec.ViewSource{SQL: `SELECT children.* FROM children`}},
			On:   []*spec.RelationLink{{ParentNamespace: "parents", ParentColumn: "id", ChildNamespace: "children", ChildColumn: "parent_id"}},
		}},
	}}
	outputType := reflect.TypeFor[[]*parent]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: outputType, DirectViewType: outputType})
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: outputType, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	type useCase struct {
		description string
		selector    *xstate.Selector
		excluded    bool
	}
	for _, tc := range []useCase{
		{description: "unrestricted"},
		{description: "selected empty relation", selector: &xstate.Selector{Fields: []string{"Children"}}},
		{description: "excluded empty relation", selector: &xstate.Selector{Fields: []string{"Name"}}, excluded: true},
		{description: "columns select empty relation", selector: &xstate.Selector{Columns: []string{"Children"}}},
	} {
		t.Run(tc.description, func(t *testing.T) {
			call, complete := dexec.ScopeOutputSelection(ctx)
			dexec.EnableLifecycleOutputSelection(call)
			dexec.BeginOutputSelection(call)
			var selectors xstate.Selectors
			if tc.selector != nil {
				selectors = xstate.Selectors{&xstate.NamedSelector{Name: "parents", Selector: *tc.selector}}
			}
			result, err := execution.Read(call, &struct{}{}, sourceGuardBinder{selectors}, nil)
			require.NoError(t, err)
			complete(result, err)
			rows := result.([]*parent)
			require.Len(t, rows, 1)
			require.Empty(t, rows[0].Children, "row values alone cannot prove relation selection")
			filter, available := dexec.CurrentOutputSelection(call, result)
			require.True(t, available)
			excluded := filter != nil && filter.ExcludePath(nil, "Children")
			require.Equal(t, tc.excluded, excluded)
		})
	}
}
