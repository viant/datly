package reader

import (
	"context"
	"database/sql"
	"strings"

	dsql "github.com/viant/datly/sql"
	sqlxread "github.com/viant/sqlx/io/read"
)

// readRetry supplies Datly's connection-error policy to the native reader.
// SQLComponent remains the authority for default and named source selection.
type readRetry struct {
	source    *dsql.SQLComponent
	connector string
}

func (r readRetry) policy() sqlxread.RetryPolicy {
	return sqlxread.RetryPolicy{Attempts: 3, Recoverable: func(err error) bool {
		return strings.Contains(err.Error(), "invalid connection")
	}, Reconnect: r.reconnect}
}

func (r readRetry) reconnect(ctx context.Context) (*sql.DB, error) {
	connection, err := r.source.Resolve(ctx, r.connector)
	return connection.DB, err
}
