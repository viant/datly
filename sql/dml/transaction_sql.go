package dml

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

// QueryContext executes immediately through the invocation-owned transaction.
// It never flushes pending buffered writes implicitly.
func (d *Data) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	owner := d.owner()
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	tx, err := owner.transaction(ctx)
	if err != nil {
		return nil, err
	}
	return tx.QueryContext(ctx, query, args...)
}

// QueryRowContext executes immediately through the invocation-owned transaction.
// It never flushes pending buffered writes implicitly.
func (d *Data) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	owner := d.owner()
	owner.executionMu.Lock()
	tx, err := owner.transaction(ctx)
	if err != nil {
		owner.executionMu.Unlock()
		return errorRow(ctx, err)
	}
	row := tx.QueryRowContext(ctx, query, args...)
	owner.executionMu.Unlock()
	return row
}

// ExecContext executes immediately through the invocation-owned transaction.
// It never flushes pending buffered writes implicitly.
func (d *Data) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	owner := d.owner()
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	tx, err := owner.transaction(ctx)
	if err != nil {
		return nil, err
	}
	return tx.ExecContext(ctx, query, args...)
}

func errorRow(ctx context.Context, err error) *sql.Row {
	db := sql.OpenDB(errorConnector{err: err})
	defer db.Close()
	return db.QueryRowContext(ctx, "SELECT 1")
}

type errorConnector struct{ err error }

func (c errorConnector) Connect(context.Context) (driver.Conn, error) { return nil, c.err }
func (c errorConnector) Driver() driver.Driver                        { return errorDriver{err: c.err} }

type errorDriver struct{ err error }

func (d errorDriver) Open(string) (driver.Conn, error) { return nil, d.err }
