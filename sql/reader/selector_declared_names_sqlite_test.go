package reader_test

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	xstate "github.com/viant/xdatly/state"
	"reflect"
	"strings"
	"testing"
)

type declaredChild struct {
	ID       int `sqlx:"id"`
	B_ID     int `sqlx:"b_id"`
	BID      int `sqlx:"bid"`
	ParentID int `sqlx:"parent_id"`
}
type declaredRoot struct {
	ID       int              `sqlx:"id"`
	B_ID     int              `sqlx:"b_id"`
	BID      int              `sqlx:"bid"`
	Children []*declaredChild `view:"children,table=children,selectorProjection=true" on:"ID:id=ParentID:parent_id"`
}

func TestReaderDeclaredNamesRootAndSubviewSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER,b_id INTEGER,bid INTEGER)", "CREATE TABLE children(id INTEGER,b_id INTEGER,bid INTEGER,parent_id INTEGER)", "INSERT INTO parents VALUES(1,2,3)", "INSERT INTO children VALUES(10,20,30,1)"))
	for _, subview := range []bool{false, true} {
		for _, source := range []string{"b.id", "b_id", "bid", "b.id AS b_id", "b.id AS bid"} {
			for _, requested := range []string{"b.id", "b_id", "bid", "id"} {
				for _, mode := range []string{"fields", "order", "mapping"} {
					t.Run(strings.Join([]string{map[bool]string{true: "child", false: "root"}[subview], source, requested, mode}, "/"), func(t *testing.T) {
						output := source
						if source == "b.id" {
							output = "id"
						}
						if at := strings.Index(source, " AS "); at >= 0 {
							output = source[at+4:]
						}
						policy := &spec.Selector{AllowFields: true, AllowOrderBy: true}
						component := &spec.Component{RootView: &spec.View{Name: "parents", Selector: policy, Source: &spec.ViewSource{SQL: "SELECT " + source + " FROM parents b"}}}
						if subview {
							component.RootView.Source.SQL = "SELECT id FROM parents"
						}
						typ := reflect.TypeFor[[]*declaredRoot]()
						plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
						require.NoError(t, err)
						child := plan.Root.View.Relations[0].Of.View
						child.Spec.Source = &spec.ViewSource{SQL: "SELECT id,parent_id FROM children"}
						child.Spec.Selector = policy
						view := plan.Root.View
						name := "parents"
						if subview {
							view = child
							name = "children"
							child.Spec.Source.SQL = "SELECT " + source + ",parent_id FROM children b"
						}
						selector := xstate.Selector{Fields: []string{requested}}
						if mode == "order" {
							selector.Fields = []string{output}
							selector.OrderBy = requested
						}
						if mode == "mapping" {
							view.Spec.Columns = []*spec.Column{{Name: requested, Source: output}}
							selector.OrderBy = requested
						}
						selectors := xstate.Selectors{&xstate.NamedSelector{Name: name, Selector: selector}}
						if subview {
							selectors = append(selectors, &xstate.NamedSelector{Name: "parents", Selector: xstate.Selector{Fields: []string{"id", "Children"}}})
						}
						execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}})
						require.NoError(t, err)
						result, err := execution.ReadResult(ctx, &struct{}{}, sourceGuardBinder{selectors}, nil)
						if mode != "mapping" && requested != output {
							require.Error(t, err)
							return
						}
						require.NoError(t, err)
						evidence, err := result.Projection.Row(0)
						require.NoError(t, err)
						if subview {
							evidence, err = evidence.RelationRow("Children", 0)
							require.NoError(t, err)
						}
						require.True(t, evidence.Fields().Known())
						field := map[string]string{"id": "ID", "b_id": "B_ID", "bid": "BID"}[output]
						require.True(t, evidence.Fields().Has(field), field)
						for _, other := range []string{"ID", "B_ID", "BID"} {
							if other != field {
								require.False(t, evidence.Fields().Has(other), other)
							}
						}
						rows := result.Data.([]*declaredRoot)
						require.Len(t, rows, 1)
						value := reflect.ValueOf(rows[0]).Elem().FieldByName(field).Int()
						if subview {
							require.Len(t, rows[0].Children, 1)
							value = reflect.ValueOf(rows[0].Children[0]).Elem().FieldByName(field).Int()
						}
						expected := map[string]int64{"b.id": 1, "b_id": 2, "bid": 3, "b.id AS b_id": 1, "b.id AS bid": 1}[source]
						if subview {
							expected *= 10
						}
						require.Equal(t, expected, value)
					})
				}
			}
		}
	}
}
