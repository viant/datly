package executor

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/metadata/info"
)

type DBSource interface {
	Db(ctx context.Context) (*sql.DB, error)
	Dialect(ctx context.Context) (*info.Dialect, error)
}
