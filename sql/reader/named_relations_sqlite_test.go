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

func TestNamedCompositeRelationsSelectionAndWindowSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx,
		`CREATE TABLE parents(tenant_id INTEGER,id INTEGER,name TEXT,visible INTEGER)`,
		`CREATE TABLE children(tenant_id INTEGER,parent_id INTEGER,child_id INTEGER,value INTEGER,visible INTEGER)`,
		`INSERT INTO parents VALUES(7,1,'first',1),(8,1,'second',1),(9,1,'hidden',0)`,
		`INSERT INTO children VALUES(7,1,10,NULL,1),(7,1,11,11,1),(7,1,12,999,0),(8,1,20,20,1),(8,1,21,21,1),(9,1,30,30,1)`))
	type child struct {
		TenantID int `sqlx:"tenant_id"`
		ParentID int `sqlx:"parent_id"`
		ChildID  int `sqlx:"child_id"`
		Value    int `sqlx:"value"`
	}
	type parent struct {
		TenantID int    `sqlx:"tenant_id"`
		ID       int    `sqlx:"id"`
		Name     string `sqlx:"name"`
		Children []*child
	}
	typ := reflect.TypeFor[[]*parent]()
	for _, strategy := range []spec.MatchStrategy{spec.MatchReadMatched, spec.MatchReadAll} {
		for _, mode := range []string{"all", "narrow", "per-parent page", "omit", "unknown field"} {
			t.Run(string(strategy)+"/"+mode, func(t *testing.T) {
				policy := &spec.Selector{AllowFields: true, AllowOrderBy: true, AllowLimit: true, AllowPage: true}
				childView := &spec.View{Name: "children", Namespace: "children", Selector: policy, Source: &spec.ViewSource{SQL: `SELECT children.* FROM (SELECT c.* FROM children c WHERE c.visible=1) children`}, Columns: []*spec.Column{{Name: "value", Type: spec.TypeRef{Name: "int"}, Nullable: true}}}
				component := &spec.Component{RootView: &spec.View{Name: "parents", Namespace: "parents", Selector: policy, Source: &spec.ViewSource{SQL: `SELECT parents.* FROM (SELECT p.* FROM parents p WHERE p.visible=1) parents`}, Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, MatchStrategy: strategy, View: childView, On: []*spec.RelationLink{{ParentNamespace: "parents", ParentColumn: "tenant_id", ChildNamespace: "children", ChildColumn: "tenant_id"}, {ParentNamespace: "parents", ParentColumn: "id", ChildNamespace: "children", ChildColumn: "parent_id"}}}}}}
				var selectors xstate.Selectors
				if mode != "all" {
					fields := []string{"Children"}
					if mode == "omit" {
						fields = []string{"name"}
						childView.Source.SQL = `SELECT children.* FROM missing_children children`
					}
					wanted := []string{"value"}
					if mode == "unknown field" {
						wanted = []string{"secret"}
					}
					childSelector := xstate.Selector{Fields: wanted, OrderBy: "child_id ASC"}
					if mode == "per-parent page" {
						childSelector.Limit = 1
						childSelector.Page = 2
					}
					selectors = xstate.Selectors{&xstate.NamedSelector{Name: "parents", Selector: xstate.Selector{Fields: fields, OrderBy: "tenant_id ASC"}}, &xstate.NamedSelector{Name: "children", Selector: childSelector}}
				}
				plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
				require.NoError(t, err)
				execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
				require.NoError(t, err)
				result, err := execution.ReadResult(ctx, &struct{}{}, sourceGuardBinder{selectors}, nil)
				if mode == "unknown field" {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				rows := result.Data.([]*parent)
				require.Len(t, rows, 2)
				for i, row := range rows {
					if mode == "omit" {
						require.Nil(t, row.Children)
						require.NotEmpty(t, row.Name)
						continue
					}
					require.Equal(t, 7+i, row.TenantID)
					require.Equal(t, 1, row.ID)
					count := 2
					if mode == "per-parent page" && strategy != spec.MatchReadAll {
						count = 1
					}
					require.Len(t, row.Children, count)
					for _, entry := range row.Children {
						require.Equal(t, row.TenantID, entry.TenantID)
						require.Equal(t, row.ID, entry.ParentID)
						require.NotEqual(t, 999, entry.Value)
					}
					if mode == "per-parent page" && strategy != spec.MatchReadAll {
						require.Equal(t, 11+i*10, row.Children[0].Value)
					}
					if mode == "narrow" || mode == "per-parent page" {
						require.Equal(t, 0, row.Children[0].ChildID)
						require.Empty(t, row.Name)
						evidence, err := result.Projection.Row(i)
						require.NoError(t, err)
						fields, err := evidence.RelationRow("Children", 0)
						require.NoError(t, err)
						require.True(t, fields.Fields().Has("Value"))
						require.True(t, fields.Fields().Has("TenantID"))
						require.True(t, fields.Fields().Has("ParentID"))
						require.False(t, fields.Fields().Has("ChildID"))
					}
				}
			})
		}
	}
}
