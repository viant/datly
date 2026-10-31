package reader_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	xreader "github.com/viant/xdatly/reader"
)

type guardPartitions struct{}

func (guardPartitions) Partitions(context.Context, xreader.PartitionRequest) ([]xreader.Partition, error) {
	return []xreader.Partition{{Expression: "id<=?", Args: []any{1}}, {Expression: "id>?", Args: []any{1}}}, nil
}
func TestQueryGuardCoversNativeRootPartitionAndRelationSQL(t *testing.T) {
	for _, partitioned := range []bool{false, true} {
		t.Run(map[bool]string{false: "relations", true: "partitions_and_relations"}[partitioned], func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER)", "CREATE TABLE children(parent_id INTEGER)", "INSERT INTO parents VALUES(1),(2)", "INSERT INTO children VALUES(1),(2)"))
			type child struct {
				ParentID int `sqlx:"parent_id"`
			}
			type parent struct {
				ID       int `sqlx:"id"`
				Children []*child
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Guarded"}, RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id FROM parents"}, Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}, View: &spec.View{Name: "children", Source: &spec.ViewSource{SQL: "SELECT parent_id FROM children WHERE $COLUMN_IN"}}}}}}
			if partitioned {
				component.RootView.Partitioning = &spec.Partitioning{Type: "guardPartitions", Concurrency: 2}
			}
			typ := reflect.TypeFor[[]*parent]()
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ, TypeLookup: func(string) (reflect.Type, error) { return reflect.TypeFor[guardPartitions](), nil }})
			require.NoError(t, err)
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
			require.NoError(t, err)
			var mu sync.Mutex
			rootCalls, childCalls := 0, 0
			guarded := execution.WithQueryGuard(func(ctx context.Context, db *sql.DB, query *xreader.ParameterizedQuery) error {
				require.Same(t, h.DB, db)
				require.NoError(t, ctx.Err())
				mu.Lock()
				defer mu.Unlock()
				if strings.Contains(query.Query, "FROM children") {
					childCalls++
					require.NotEmpty(t, query.Args)
				} else {
					rootCalls++
					if partitioned {
						require.NotEmpty(t, query.Args)
					}
				}
				return nil
			})
			value, err := guarded.Read(ctx, &struct{}{}, nil, nil)
			require.NoError(t, err)
			require.Len(t, value, 2)
			require.GreaterOrEqual(t, childCalls, 1)
			if partitioned {
				require.Equal(t, 2, rootCalls)
			} else {
				require.Equal(t, 1, rootCalls)
			}
			denied := errors.New("resource budget exceeded")
			rejected := execution.WithQueryGuard(func(context.Context, *sql.DB, *xreader.ParameterizedQuery) error { return denied })
			_, err = rejected.Read(ctx, &struct{}{}, nil, nil)
			require.ErrorIs(t, err, denied)
			// Attaching a guard never mutates the original registered execution.
			value, err = execution.Read(ctx, &struct{}{}, nil, nil)
			require.NoError(t, err)
			require.Len(t, value, 2)
		})
	}
}

func TestQueryGuardCarriesIntoPreparedProjection(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1),(2)"))
	type row struct {
		ID int `sqlx:"id"`
	}
	component := &spec.Component{RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}}
	typ := reflect.TypeFor[[]*row]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
	require.NoError(t, err)
	calls := 0
	deny := false
	rejection := errors.New("projection budget exceeded")
	guarded := execution.WithQueryGuard(func(context.Context, *sql.DB, *xreader.ParameterizedQuery) error {
		calls++
		if deny {
			return rejection
		}
		return nil
	})
	prepared, err := guarded.(dexec.QueryPreparer).PrepareQuery(ctx, &struct{}{}, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 0, calls)
	value, err := prepared.Projection.ReadProjection(ctx, dexec.ProjectionRequest{SQL: "SELECT id FROM (" + prepared.SQL + ") projected", Args: prepared.Args, RowType: reflect.TypeFor[row]()})
	require.NoError(t, err)
	require.Len(t, value, 2)
	require.Equal(t, 1, calls)
	deny = true
	_, err = prepared.Projection.ReadProjection(ctx, dexec.ProjectionRequest{SQL: "SELECT * FROM missing_records", RowType: reflect.TypeFor[row]()})
	require.ErrorIs(t, err, rejection)
	require.Equal(t, 2, calls)
}
