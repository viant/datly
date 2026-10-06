package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

// Preserve QueryRowContext's standard Scan error boundary without dispatching
// a rejected query or exposing an actual connector/transaction.
func guardedQueryErrorRow(ctx context.Context, err error) *sql.Row {
	db := sql.OpenDB(guardedQueryErrorConnector{err: err})
	defer db.Close()
	return db.QueryRowContext(ctx, "SELECT 1")
}

type guardedQueryErrorConnector struct{ err error }

func (c guardedQueryErrorConnector) Connect(context.Context) (driver.Conn, error) { return nil, c.err }
func (c guardedQueryErrorConnector) Driver() driver.Driver {
	return guardedQueryErrorDriver{err: c.err}
}

type guardedQueryErrorDriver struct{ err error }

func (d guardedQueryErrorDriver) Open(string) (driver.Conn, error) { return nil, d.err }
