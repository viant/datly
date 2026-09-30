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

type lockedRow struct {
	ID   int    `sqlx:"id"`
	Name string `sqlx:"name"`
}

func TestReaderLockUsesActiveTransactionSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT)"))
	component := &spec.Component{RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: `SELECT id,name FROM records ORDER BY id ${View.ForUpdate()}`}}}
	output := reflect.TypeFor[[]*lockedRow]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, DirectViewType: output})
	require.NoError(t, err)
	newExecution := func(source *dsql.SQLComponent) *reader.Execution {
		execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, Plan: plan, SQL: source})
		require.NoError(t, err)
		return execution
	}
	_, err = newExecution(&dsql.SQLComponent{DB: h.DB}).ReadResult(ctx, &struct{}{}, sourceGuardBinder{}, nil)
	require.ErrorContains(t, err, "active transaction")
	tx, err := h.DB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(1,'uncommitted')")
	require.NoError(t, err)
	result, err := newExecution(&dsql.SQLComponent{DB: h.DB, Tx: tx}).ReadResult(ctx, &struct{}{}, sourceGuardBinder{}, nil)
	require.NoError(t, err)
	rows := result.Data.([]*lockedRow)
	require.Len(t, rows, 1)
	require.Equal(t, "uncommitted", rows[0].Name)
	require.NoError(t, tx.Rollback())
	var count int
	require.NoError(t, h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count))
	require.Zero(t, count)
}
