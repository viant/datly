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
)

func TestReaderRootAndRelationsUseCallerTransactionSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx,
		"CREATE TABLE parents(id INTEGER,name TEXT,extra INTEGER,secret TEXT)",
		"CREATE TABLE children(id INTEGER,parent_id INTEGER,name TEXT,secret TEXT)"))
	tx, err := h.DB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, "INSERT INTO parents VALUES(1,'uncommitted',0,'')")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "INSERT INTO children VALUES(10,1,'child','')")
	require.NoError(t, err)
	component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT * FROM parents $WHERE_CRITERIA ORDER BY id"}}}
	output := reflect.TypeFor[[]*sourceGuardRow]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, DirectViewType: output})
	require.NoError(t, err)
	plan.Root.View.Relations[0].Of.View.Spec.Source = &spec.ViewSource{SQL: "SELECT * FROM children $WHERE_CRITERIA ORDER BY id"}
	newExecution := func(source *dsql.SQLComponent) *reader.Execution {
		execution, buildErr := reader.NewExecution(reader.Config{Component: component,
			InputType: reflect.TypeFor[struct{}](), OutputType: output, Plan: plan, SQL: source})
		require.NoError(t, buildErr)
		return execution
	}
	result, err := newExecution(&dsql.SQLComponent{DB: h.DB, Tx: tx}).ReadResult(ctx, &struct{}{}, sourceGuardBinder{}, nil)
	require.NoError(t, err)
	rows := result.Data.([]*sourceGuardRow)
	require.Len(t, rows, 1)
	require.Equal(t, "uncommitted", rows[0].Name)
	require.Len(t, rows[0].Children, 1)
	require.Equal(t, "child", rows[0].Children[0].Name)
	require.NoError(t, tx.Rollback())
	result, err = newExecution(&dsql.SQLComponent{DB: h.DB}).ReadResult(ctx, &struct{}{}, sourceGuardBinder{}, nil)
	require.NoError(t, err)
	require.Empty(t, result.Data.([]*sourceGuardRow))
}
