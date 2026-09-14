package jobs

import (
	"context"
	"database/sql"
	"fmt"
	sqlconfig "github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/info"
)

// SQLConfig selects the configured job connector and original-shaped job table.
// Missing tables are created by SQLX unless DisableTableCreation is set.
// Existing tables are never altered or rewritten.
type SQLConfig struct {
	DB                   *sql.DB
	Dialect              *info.Dialect
	Table                string
	Dataset              string
	DisableTableCreation bool
}

func (c SQLConfig) resolve(ctx context.Context) (*sql.DB, *info.Dialect, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, "", err
	}
	if c.DB == nil {
		return nil, nil, "", fmt.Errorf("configured job database is required")
	}
	dialect := c.Dialect
	if dialect == nil {
		var err error
		dialect, err = sqlconfig.Dialect(ctx, c.DB)
		if err != nil {
			return nil, nil, "", err
		}
	}
	table, err := c.table(dialect)
	return c.DB, dialect, table, err
}
func (c SQLConfig) table(dialect *info.Dialect) (string, error) {
	table := c.Table
	if table == "" {
		table = "DATLY_JOBS"
	}
	name, err := dialect.TableIdentifier(table, c.Dataset)
	if err != nil {
		return "", fmt.Errorf("job table configuration: %w", err)
	}
	return name, nil
}
