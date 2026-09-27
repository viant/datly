package reader

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	sqlxio "github.com/viant/sqlx/io"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
)

// Model MySQL's single active result stream: another prepare while rows remain
// open poisons the connection, even though individual driver calls are serialized.
type cursorConnector struct{ overlaps atomic.Int32 }
type cursorDriver struct{}
type cursorConn struct {
	owner *cursorConnector
	open  bool
}
type cursorTx struct{}
type cursorStmt struct{ conn *cursorConn }
type cursorRows struct {
	conn    *cursorConn
	emitted bool
}

func (c *cursorConnector) Connect(context.Context) (driver.Conn, error) {
	return &cursorConn{owner: c}, nil
}
func (*cursorConnector) Driver() driver.Driver        { return &cursorDriver{} }
func (cursorDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }
func (c *cursorConn) Prepare(string) (driver.Stmt, error) {
	if c.open {
		c.owner.overlaps.Add(1)
		return nil, driver.ErrBadConn
	}
	return &cursorStmt{conn: c}, nil
}
func (*cursorConn) Close() error                               { return nil }
func (*cursorConn) Begin() (driver.Tx, error)                  { return cursorTx{}, nil }
func (cursorTx) Commit() error                                 { return nil }
func (cursorTx) Rollback() error                               { return nil }
func (*cursorStmt) Close() error                               { return nil }
func (*cursorStmt) NumInput() int                              { return 0 }
func (*cursorStmt) Exec([]driver.Value) (driver.Result, error) { return driver.RowsAffected(1), nil }
func (s *cursorStmt) Query([]driver.Value) (driver.Rows, error) {
	s.conn.open = true
	return &cursorRows{conn: s.conn}, nil
}
func (*cursorRows) Columns() []string { return []string{"id"} }
func (r *cursorRows) Close() error    { r.conn.open = false; return nil }
func (r *cursorRows) Next(values []driver.Value) error {
	if r.emitted {
		return io.EOF
	}
	r.emitted = true
	values[0] = int64(1)
	return nil
}
func (*cursorRows) ColumnTypeScanType(int) reflect.Type   { return reflect.TypeFor[int64]() }
func (*cursorRows) ColumnTypeDatabaseTypeName(int) string { return "INTEGER" }

type cursorRow struct {
	ID int `sqlx:"id"`
}

func cursorRead(ctx context.Context, db *sql.DB, tx *sql.Tx, visit func(any) error, options ...sqlxread.Option) error {
	session := &Session{}
	session.initMetrics(nil)
	view := &data.View{Spec: spec.View{Name: "cursor"}}
	r := rowRead{newRow: func() any { return &cursorRow{} }, options: options}
	return r.query(ctx, rowQuery{db: db, tx: tx, query: &cache.ParmetrizedQuery{SQL: "SELECT id"}, visit: visit, read: session.beginView(ctx, view)})
}

func TestTransactionCursorSerialization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	driver := &cursorConnector{}
	db := sql.OpenDB(driver)
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	opened, drain := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(drain) }) }
	defer unblock()
	first := make(chan error, 1)
	go func() {
		first <- cursorRead(ctx, db, tx, func(any) error { return nil }, sqlxread.WithColumnsObserver(func([]sqlxio.Column) error {
			close(opened)
			select {
			case <-drain:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}))
	}()
	select {
	case <-opened:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Another connection remains usable while this transaction's cursor is held.
	other, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, cursorRead(ctx, db, other, func(any) error { return nil }))
	require.NoError(t, other.Rollback())
	require.NoError(t, cursorRead(ctx, db, nil, func(any) error { return nil }))
	waitCtx, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	defer stop()
	require.ErrorIs(t, cursorRead(waitCtx, db, tx, func(any) error { return nil }), context.DeadlineExceeded)
	const workers = 12
	results := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() { results <- cursorRead(ctx, db, tx, func(any) error { return nil }) }()
	}
	unblock()
	require.NoError(t, <-first)
	for i := 0; i < workers; i++ {
		require.NoError(t, <-results)
	}
	require.Zero(t, driver.overlaps.Load())
	// A row callback may reenter the same transaction after its cursor closes.
	require.NoError(t, cursorRead(ctx, db, tx, func(any) error {
		return cursorRead(ctx, db, tx, func(any) error { return nil })
	}))
	abort := errors.New("hook failure")
	require.ErrorIs(t, cursorRead(ctx, db, tx, func(any) error { return abort }), abort)
	require.ErrorIs(t, cursorRead(ctx, db, tx, func(any) error { return nil }, sqlxread.WithColumnsObserver(func([]sqlxio.Column) error {
		return abort
	})), abort)
	require.Panics(t, func() { _ = cursorRead(ctx, db, tx, func(any) error { panic("hook panic") }) })
	require.Panics(t, func() {
		_ = cursorRead(ctx, db, tx, func(any) error { return nil }, sqlxread.WithColumnsObserver(func([]sqlxio.Column) error {
			panic("scan panic")
		}))
	})
	require.NoError(t, cursorRead(ctx, db, tx, func(any) error { return nil }))
}
