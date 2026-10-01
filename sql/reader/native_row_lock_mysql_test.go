package reader_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	_ "github.com/viant/sqlx/metadata/product/mysql"
)

func TestNativePhysicalRowLockIndependentPoolsMySQL(t *testing.T) {
	dsn := os.Getenv("SQLX_SCOPED_MYSQL_DSN")
	if dsn == "" {
		t.Skip("live MySQL fixture not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	open := func() *sql.DB {
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		db.SetMaxOpenConns(3)
		require.NoError(t, db.PingContext(ctx))
		t.Cleanup(func() { db.Close() })
		return db
	}
	first, second := open(), open()
	table := fmt.Sprintf("datly_native_lock_%d", time.Now().UnixNano())
	_, err := first.ExecContext(ctx, "CREATE TABLE "+table+"(id INTEGER PRIMARY KEY,name VARCHAR(32)) ENGINE=InnoDB")
	require.NoError(t, err)
	t.Cleanup(func() { first.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table) })
	_, err = first.ExecContext(ctx, "INSERT INTO "+table+" VALUES(1,'original'),(2,'unrelated')")
	require.NoError(t, err)
	newExecution := func(db *sql.DB, tx *sql.Tx, id int) *reader.Execution {
		component := &spec.Component{RootView: &spec.View{Name: "records", RowLock: table + " r", RowLockOrder: "r.id", Source: &spec.ViewSource{SQL: fmt.Sprintf("SELECT data_rows.id,data_rows.name FROM (SELECT r.id,r.name FROM %s r WHERE r.id=%d AND EXISTS(SELECT r.id FROM %s r WHERE r.id=1)) data_rows", table, id, table)}}}
		output := reflect.TypeFor[[]*lockedRow]()
		plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, DirectViewType: output})
		require.NoError(t, err)
		e, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, Plan: plan, SQL: &dsql.SQLComponent{DB: db, Tx: tx}})
		require.NoError(t, err)
		return e
	}
	read := func(e *reader.Execution) ([]*lockedRow, error) {
		v, err := e.Read((dexec.ReaderOptions{ForUpdate: []string{dexec.RootView}}).Context(ctx), &struct{}{}, nil, nil)
		if err != nil {
			return nil, err
		}
		return v.([]*lockedRow), nil
	}
	tx1, err := first.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)
	defer tx1.Rollback()
	rows, err := read(newExecution(first, tx1, 1))
	require.NoError(t, err)
	require.Equal(t, "original", rows[0].Name)
	tx2, err := second.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)
	defer tx2.Rollback()
	var before string
	require.NoError(t, tx2.QueryRowContext(ctx, "SELECT name FROM "+table+" WHERE id=1").Scan(&before))
	require.Equal(t, "original", before, "preexisting repeatable-read snapshot")
	_, err = tx1.ExecContext(ctx, "UPDATE "+table+" SET name='committed' WHERE id=1")
	require.NoError(t, err)
	txOther, err := first.BeginTx(ctx, nil)
	require.NoError(t, err)
	other, err := read(newExecution(first, txOther, 2))
	require.NoError(t, err)
	require.Equal(t, "unrelated", other[0].Name)
	require.NoError(t, txOther.Rollback())
	contender := newExecution(second, tx2, 1)
	type result struct {
		rows []*lockedRow
		err  error
	}
	done := make(chan result, 1)
	started := make(chan struct{})
	go func() { close(started); r, e := read(contender); done <- result{r, e} }()
	<-started
	select {
	case got := <-done:
		t.Fatalf("contending physical lock returned before owner commit: error=%v rows=%d", got.err, len(got.rows))
	case <-time.After(250 * time.Millisecond):
	}
	require.NoError(t, tx1.Commit())
	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.Len(t, got.rows, 1)
		require.Equal(t, "committed", got.rows[0].Name, "locking read bypasses earlier consistent snapshot")
	case <-ctx.Done():
		t.Fatal("contender did not resume after commit")
	}
	_, err = tx2.ExecContext(ctx, "UPDATE "+table+" SET name='rolledback' WHERE id=1")
	require.NoError(t, err, "native read retained caller transaction ownership")
	require.NoError(t, tx2.Rollback())
	var after string
	require.NoError(t, first.QueryRowContext(ctx, "SELECT name FROM "+table+" WHERE id=1").Scan(&after))
	require.Equal(t, "committed", after)
}
