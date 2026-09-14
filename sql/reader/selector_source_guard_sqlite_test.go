package reader_test

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
	"reflect"
	"testing"
)

type sourceGuardBinder struct{ selectors xstate.Selectors }

func (b sourceGuardBinder) Bind(context.Context, any) error { return nil }
func (b sourceGuardBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	return b.selectors, key == xhandler.SelectorsKey, nil
}

type sourceGuardChild struct {
	ID       int    `sqlx:"id"`
	ParentID int    `sqlx:"parent_id"`
	Name     string `sqlx:"name"`
	Secret   string `sqlx:"secret"`
}
type sourceGuardRow struct {
	ID       int                 `sqlx:"id"`
	Name     string              `sqlx:"name"`
	Extra    int                 `sqlx:"extra"`
	Secret   string              `sqlx:"secret"`
	Children []*sourceGuardChild `view:"children,table=children,selectorProjection=true" on:"ID:id=ParentID:parent_id"`
}

func TestSelectorSourceGuardReaderSQLite(t *testing.T) {
	for _, tc := range []struct {
		name, sql, view, order string
		fields, known, absent  []string
		reject                 string
	}{
		{name: "root selected known", sql: "SELECT id,name FROM parents", fields: []string{"name"}, order: "1 DESC", known: []string{"Name"}, absent: []string{"ID", "Secret"}},
		{name: "root stale metadata field", sql: "SELECT id,name FROM parents", fields: []string{"secret"}, reject: "not found column"},
		{name: "root allowlist cannot widen", sql: "SELECT id,name FROM parents", order: "secret", reject: "source projection"},
		{name: "root order alias cannot widen", sql: "SELECT id,name FROM parents", order: "private", reject: "source projection"},
		{name: "root ordinal bound", sql: "SELECT id,name FROM parents", fields: []string{"name"}, order: "2", reject: "outside source projection"},
		{name: "derived excludes stale fields", sql: "SELECT p.* FROM (SELECT id,name FROM parents) p", fields: []string{"secret"}, reject: "not found column"},
		{name: "mixed derived known", sql: "SELECT p.*,7 AS extra FROM (SELECT id,name FROM parents) p", fields: []string{"extra", "name"}, order: "1 DESC", known: []string{"Name", "Extra"}, absent: []string{"ID", "Secret"}},
		{name: "child known", sql: "SELECT id,name FROM parents", view: "children", fields: []string{"name"}, order: "name DESC", known: []string{"ParentID", "Name"}, absent: []string{"ID", "Secret"}},
		{name: "child invalid field", sql: "SELECT id,name FROM parents", view: "children", fields: []string{"secret"}, reject: "not found column"},
		{name: "child invalid order", sql: "SELECT id,name FROM parents", view: "children", order: "private", reject: "source projection"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER,name TEXT,secret TEXT)", "CREATE TABLE children(id INTEGER,parent_id INTEGER,name TEXT,secret TEXT)", "INSERT INTO parents VALUES(1,'first','hidden'),(2,'second','hidden')", "INSERT INTO children VALUES(10,1,'child','hidden'),(20,2,'other','hidden')"))
			policy := &spec.Selector{AllowFields: true, AllowOrderBy: true, Orderable: []spec.FieldPath{"id", "name", "extra", "secret"}, OrderAliases: map[string]spec.FieldPath{"private": "secret"}}
			component := &spec.Component{RootView: &spec.View{Name: "parents", Selector: policy, Source: &spec.ViewSource{SQL: tc.sql}}}
			output := reflect.TypeFor[[]*sourceGuardRow]()
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, DirectViewType: output})
			require.NoError(t, err)
			child := plan.Root.View.Relations[0].Of.View
			child.Spec.Source = &spec.ViewSource{SQL: "SELECT id,parent_id,name FROM children"}
			child.Spec.Selector = policy
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}})
			require.NoError(t, err)
			name := tc.view
			if name == "" {
				name = "parents"
			}
			selectors := xstate.Selectors{&xstate.NamedSelector{Name: name, Selector: xstate.Selector{Fields: tc.fields, OrderBy: tc.order}}}
			result, err := execution.ReadResult(ctx, &struct{}{}, sourceGuardBinder{selectors}, nil)
			if tc.reject != "" {
				require.ErrorContains(t, err, tc.reject)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result.Projection)
			evidence, err := result.Projection.Row(0)
			require.NoError(t, err)
			if tc.view != "" {
				evidence, err = evidence.RelationRow("Children", 0)
				require.NoError(t, err)
			}
			require.True(t, evidence.Fields().Known())
			for _, name := range tc.known {
				require.True(t, evidence.Fields().Has(name), name)
			}
			for _, name := range tc.absent {
				require.False(t, evidence.Fields().Has(name), name)
			}
			rows := result.Data.([]*sourceGuardRow)
			require.Len(t, rows, 2)
			if tc.name == "root selected known" || tc.name == "mixed derived known" {
				require.Equal(t, "second", rows[0].Name)
			}
		})
	}
}

func TestSelectorSourceGuardCubeSQLite(t *testing.T) {
	type row struct {
		Name   string `sqlx:"name"`
		Total  int    `sqlx:"total"`
		Secret string `sqlx:"secret"`
	}
	for _, tc := range []struct {
		fields        []string
		order, reject string
	}{{[]string{"total"}, "1 DESC", ""}, {[]string{"total"}, "2", "outside source projection"}, {[]string{"secret"}, "", "not found column"}, {nil, "secret", "source projection"}} {
		t.Run(tc.order+tc.reject, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE sales(name TEXT,price INTEGER)", "INSERT INTO sales VALUES('a',2),('b',3)"))
			groupable := true
			component := &spec.Component{RootView: &spec.View{Name: "cube", Groupable: &groupable, Selector: &spec.Selector{AllowFields: true, AllowOrderBy: true, Orderable: []spec.FieldPath{"total", "secret"}}, Source: &spec.ViewSource{SQL: "SELECT name,SUM(price) AS total FROM sales GROUP BY name"}}}
			output := reflect.TypeFor[[]*row]()
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, DirectViewType: output})
			require.NoError(t, err)
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}})
			require.NoError(t, err)
			result, err := execution.ReadResult(ctx, &struct{}{}, sourceGuardBinder{xstate.Selectors{&xstate.NamedSelector{Name: "cube", Selector: xstate.Selector{Fields: tc.fields, OrderBy: tc.order}}}}, nil)
			if tc.reject != "" {
				require.ErrorContains(t, err, tc.reject)
				return
			}
			require.NoError(t, err)
			rows := result.Data.([]*row)
			require.Len(t, rows, 1)
			require.Equal(t, 5, rows[0].Total)
			evidence, err := result.Projection.Row(0)
			require.NoError(t, err)
			require.True(t, evidence.Fields().Known())
			require.True(t, evidence.Fields().Has("Total"))
			require.False(t, evidence.Fields().Has("Name"))
			require.False(t, evidence.Fields().Has("Secret"))
		})
	}
}
