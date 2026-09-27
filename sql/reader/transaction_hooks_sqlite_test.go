package reader_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
)

type transactionHookKey struct{}
type transactionHookRow struct {
	ID    int                    `sqlx:"-"`
	Name  string                 `sqlx:"name"`
	Child *transactionCacheChild `view:"children" on:"ID:id=ParentID:parent_id" sql:"SELECT id,parent_id,name FROM children WHERE $COLUMN_IN"`
}

func (r *transactionHookRow) OnFetch(ctx context.Context) error {
	if err := ctx.Value(transactionHookKey{}).(func(context.Context) error)(ctx); err != nil {
		return err
	}
	r.Name = "hook:" + r.Name
	return nil
}

func TestTransactionalBufferedRowHooksAndCapturedKeysSQLite(t *testing.T) {
	for _, pointers := range []bool{false, true} {
		t.Run(fmt.Sprintf("pointers=%v", pointers), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			h := sqlite.New(t)
			require.NoError(t, h.ExecStatements(ctx,
				"CREATE TABLE parents(id INTEGER,name TEXT)",
				"WITH RECURSIVE ids(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM ids WHERE id<300) INSERT INTO parents SELECT id,'row' FROM ids",
				"CREATE TABLE children(id INTEGER,parent_id INTEGER,name TEXT)",
				"INSERT INTO children SELECT id,id,'child' FROM parents"))
			tx, err := h.DB.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			source := &dsql.SQLComponent{DB: h.DB, Tx: tx}
			build := func(output reflect.Type, query string) *reader.Execution {
				component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: query}}}
				plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, DirectViewType: output})
				require.NoError(t, err)
				for _, relation := range plan.Root.Relations {
					relation.Target.View.Spec.BatchSize = 17
					relation.Target.View.Spec.BatchConcurrency = 4
				}
				execution, err := reader.NewExecution(reader.Config{Component: component, Plan: plan, InputType: reflect.TypeFor[struct{}](), OutputType: output, SQL: source})
				require.NoError(t, err)
				return execution
			}
			type lookup struct {
				ID int `sqlx:"id"`
			}
			inner := build(reflect.TypeFor[[]lookup](), "SELECT 1 AS id")
			calls := 0
			ctx = context.WithValue(ctx, transactionHookKey{}, func(ctx context.Context) error {
				calls++
				_, err := inner.Read(ctx, &struct{}{}, nil, nil)
				return err
			})
			output := reflect.TypeFor[[]transactionHookRow]()
			if pointers {
				output = reflect.TypeFor[[]*transactionHookRow]()
			}
			execution := build(output, "SELECT id,name FROM parents ORDER BY id")
			value, err := execution.Read(ctx, &struct{}{}, nil, nil)
			require.NoError(t, err)
			require.Equal(t, 300, calls)
			rows := reflect.ValueOf(value)
			require.Equal(t, 300, rows.Len())
			for i := 0; i < rows.Len(); i++ {
				row := rows.Index(i)
				if pointers {
					row = row.Elem()
				}
				require.Equal(t, "hook:row", row.FieldByName("Name").String(), "row %d", i)
				child := row.FieldByName("Child").Interface().(*transactionCacheChild)
				require.NotNil(t, child)
				require.Equal(t, i+1, child.ParentID)
			}
		})
	}
}

func TestTransactionalBufferedCodecValueRowsSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,tags TEXT)",
		"WITH RECURSIVE ids(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM ids WHERE id<300) INSERT INTO records SELECT id,'a,b' FROM ids"))
	tx, err := h.DB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	type output struct{ Rows []codecTestRow }
	component := &spec.Component{RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id,tags FROM records ORDER BY id"}}}
	factory := &splitCodecFactory{}
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), DirectViewField: "Rows", CodecFactory: factory})
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, Plan: plan, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), SQL: &dsql.SQLComponent{DB: h.DB, Tx: tx}})
	require.NoError(t, err)
	value, err := execution.Read(ctx, &struct{}{}, nil, nil)
	require.NoError(t, err)
	rows := value.(*output).Rows
	require.Len(t, rows, 300)
	for i, row := range rows {
		require.Equal(t, codecTestRow{ID: i + 1, Tags: []string{"a", "b"}, Fetched: true, Seen: "a|b"}, row)
	}
	require.EqualValues(t, 300, factory.calls.Load())
}
