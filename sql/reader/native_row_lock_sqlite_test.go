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
)

func TestNativeReaderRowLockOptionsSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT)", "INSERT INTO records VALUES(2,'second'),(1,'first')"))
	component := &spec.Component{RootView: &spec.View{Name: "records", RowLock: "records r", RowLockOrder: "r.id", Source: &spec.ViewSource{SQL: `SELECT r.id,r.name FROM records r ORDER BY r.id DESC`}}}
	output := reflect.TypeFor[[]*lockedRow]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, DirectViewType: output})
	require.NoError(t, err)
	newExecution := func(source *dsql.SQLComponent) *reader.Execution {
		e, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, Plan: plan, SQL: source})
		require.NoError(t, err)
		return e
	}
	e := newExecution(&dsql.SQLComponent{DB: h.DB})
	value, err := e.Read(ctx, &struct{}{}, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 2, value.([]*lockedRow)[0].ID, "ordinary ordering unchanged")
	locked := (dexec.ReaderOptions{ForUpdate: []string{dexec.RootView}}).Context(ctx)
	_, err = e.Read(locked, &struct{}{}, nil, nil)
	require.ErrorContains(t, err, "active transaction")
	tx, err := h.DB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, "UPDATE records SET name='pending' WHERE id=1")
	require.NoError(t, err)
	e = newExecution(&dsql.SQLComponent{DB: h.DB, Tx: tx})
	value, err = e.Read(locked, &struct{}{}, nil, nil)
	require.NoError(t, err)
	rows := value.([]*lockedRow)
	require.Equal(t, 1, rows[0].ID)
	require.Equal(t, "pending", rows[0].Name)
	query, err := e.PrepareQuery(locked, &struct{}{}, nil, nil)
	require.NoError(t, err)
	require.NotContains(t, query.SQL, "FOR UPDATE")
	require.Contains(t, query.SQL, "ORDER BY r.id ASC")
	_, err = e.Read((dexec.ReaderOptions{ForUpdate: []string{"unknown"}}).Context(ctx), &struct{}{}, nil, nil)
	require.Error(t, err)
	_, err = e.Read((dexec.ReaderOptions{ForUpdate: []string{dexec.RootView, dexec.RootView}}).Context(ctx), &struct{}{}, nil, nil)
	require.ErrorContains(t, err, "twice")
	_, err = e.Read((dexec.ReaderOptions{ForUpdate: []string{dexec.RootView}, CacheOnly: true}).Context(ctx), &struct{}{}, nil, nil)
	require.ErrorContains(t, err, "cache-only")
	require.NoError(t, tx.Rollback())
	var name string
	require.NoError(t, h.DB.QueryRowContext(ctx, "SELECT name FROM records WHERE id=1").Scan(&name))
	require.Equal(t, "first", name, "caller owns rollback")
	plan.Root.View.Spec.RowLock = ""
	_, err = e.Read(locked, &struct{}{}, nil, nil)
	require.ErrorContains(t, err, "no declared capability")
}
func TestNativeReaderRowLocksBypassSharedCachesSQLite(t *testing.T) {
	f := newTransactionCacheFixture(t, true)
	f.plan.Root.View.Spec.RowLock = "parents"
	_, err := f.read(context.Background())
	require.NoError(t, err)
	ctx, tx := f.transaction(t)
	_, err = tx.ExecContext(ctx, "UPDATE parents SET name='pending';UPDATE children SET name='pending'")
	require.NoError(t, err)
	before := map[string][2]int64{}
	for name, p := range f.probes {
		before[name] = [2]int64{p.reads.Load(), p.writes.Load()}
	}
	out, err := f.read((dexec.ReaderOptions{ForUpdate: []string{dexec.RootView}, RefreshCache: true}).Context(ctx))
	require.NoError(t, err)
	assertTransactionCacheOutput(t, out, "pending")
	for name, p := range f.probes {
		if name == "external" {
			continue
		}
		require.Equal(t, before[name], [2]int64{p.reads.Load(), p.writes.Load()}, name)
	}
}
