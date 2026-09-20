package handler

import (
	"context"
	"database/sql"

	xhandler "github.com/viant/xdatly/handler"
)

const (
	// TransactionSQLCapabilityKey identifies connector-scoped immediate SQL
	// execution through Datly's invocation-owned transaction unit.
	TransactionSQLCapabilityKey xhandler.ValueKey = "transactionSQL"
)

// TransactionSQL exposes immediate SQL execution inside Datly's managed
// invocation transaction. It deliberately does not expose Commit or Rollback.
type TransactionSQL interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// TransactionSQLProvider resolves a connector name to transaction-scoped SQL.
// Each connector maps to its own Datly database unit; this is not distributed
// transaction coordination across connectors.
type TransactionSQLProvider interface {
	Connector(ctx context.Context, name string) (TransactionSQL, error)
}
