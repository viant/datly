package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	sqlite "github.com/mattn/go-sqlite3"
	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type groupStartGate struct {
	active                     atomic.Bool
	entered, release           chan struct{}
	once                       sync.Once
	begins, rollbacks, commits atomic.Int64
}
type groupStartSQLiteDriver struct{ gate *groupStartGate }
type groupStartConn struct {
	*sqlite.SQLiteConn
	gate *groupStartGate
}
type groupStartTx struct {
	driver.Tx
	gate *groupStartGate
}

var groupStartDriverSequence atomic.Uint64

func (d *groupStartSQLiteDriver) Open(dsn string) (driver.Conn, error) {
	raw, err := new(sqlite.SQLiteDriver).Open(dsn)
	if err != nil {
		return nil, err
	}
	return &groupStartConn{raw.(*sqlite.SQLiteConn), d.gate}, nil
}
func (c *groupStartConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.gate.active.Load() {
		c.gate.begins.Add(1)
		c.gate.once.Do(func() { close(c.gate.entered) })
		select {
		case <-c.gate.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	tx, err := c.SQLiteConn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &groupStartTx{tx, c.gate}, nil
}
func (c *groupStartConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (tx *groupStartTx) Rollback() error { tx.gate.rollbacks.Add(1); return tx.Tx.Rollback() }
func (tx *groupStartTx) Commit() error   { tx.gate.commits.Add(1); return tx.Tx.Commit() }
func TestResolutionGroupNativeStartWinsRaceThenActualRootRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	gate := &groupStartGate{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate.release) }) }
	defer release()
	name := fmt.Sprintf("group-start-sqlite-%d", groupStartDriverSequence.Add(1))
	sql.Register(name, &groupStartSQLiteDriver{gate})
	db, err := sql.Open(name, filepath.Join(t.TempDir(), "start.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	scope := newDataScope(dml.Source{DB: db})
	if err = scope.enrollBufferedScope(ctx); err != nil {
		t.Fatal(err)
	}
	data, err := scope.resolve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	native := data.(*dml.Data)
	if err = data.Execute("INSERT INTO records VALUES(101,'queued must not execute')"); err != nil {
		t.Fatal(err)
	}
	gate.active.Store(true)
	done := make(chan error, 1)
	go func() { done <- native.Start(ctx) }()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("actual BeginTx not entered")
	}
	_, openErr := drainowner.OpenBindingGroup(scope.nativeInvocation, []drainowner.BindingGroupMember{{Path: "path"}})
	if !errors.Is(openErr, drainowner.ErrBindingGroup) || !errors.Is(drainowner.ProtectedFailure(scope.nativeInvocation), openErr) {
		t.Fatalf("startup winner allowed group or lost immediate terminal cause %v", openErr)
	}
	// The driver operation was already admitted before Open; its actual outcome
	// belongs to existing native completion, with no retroactive fake cancellation.
	release()
	if err = <-done; err != nil {
		t.Fatalf("already admitted actual startup failed %v", err)
	}
	if _, tx := native.InvocationTransaction(); tx == nil {
		t.Fatal("actual driver BeginTx outcome missing")
	}
	if err = completeDataScope(ctx, scope, true, openErr); !errors.Is(err, openErr) {
		t.Fatalf("root lost overlap cause %v", err)
	}
	if outcome := native.TransactionOutcome(); outcome.State != xhandler.TransactionRolledBack {
		t.Fatalf("owned outcome not actual rollback %+v", outcome)
	}
	if gate.begins.Load() != 1 || gate.rollbacks.Load() != 1 || gate.commits.Load() != 0 {
		t.Fatalf("actual Tx effects begin%d rollback%d commit%d", gate.begins.Load(), gate.rollbacks.Load(), gate.commits.Load())
	}
	var count int
	if err = db.QueryRowContext(ctx, "SELECT count(*) FROM records").Scan(&count); err != nil || count != 0 {
		t.Fatalf("queued buffer executed count%d err%v", count, err)
	}
}
