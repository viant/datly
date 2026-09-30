package standalone

import (
	"context"
	"fmt"
	"strings"

	sqlconfig "github.com/viant/sqlx/io/config"
)

// ColumnInfo is live connector schema metadata for application startup checks.
// A nil Length means the dialect does not declare a character limit.
type ColumnInfo struct {
	Dialect string
	Length  *int64
}

// InspectColumn reads one column through SQLX's metadata service. Application
// callers receive metadata only; no raw database handle or SQL execution API
// crosses the standalone runtime boundary.
func (s *Server) InspectColumn(ctx context.Context, connectorName, table, column string) (*ColumnInfo, error) {
	if s == nil || s.source == nil || s.source.connections == nil || ctx == nil {
		return nil, fmt.Errorf("linked connector and context are required")
	}
	if strings.TrimSpace(table) == "" || strings.TrimSpace(column) == "" {
		return nil, fmt.Errorf("table and column names are required")
	}
	connections := s.source.connections
	db, err := connections.ResolveDB(ctx, connectorName)
	if err != nil {
		return nil, err
	}
	dialect, err := connections.SQL.Dialect(ctx)
	if err != nil {
		return nil, err
	}
	if dialect == nil {
		return nil, fmt.Errorf("connector dialect is unavailable")
	}
	session, err := sqlconfig.Session(ctx, db, dialect)
	if err != nil {
		return nil, err
	}
	columns, err := sqlconfig.Columns(ctx, session, db, table, dialect)
	if err != nil {
		return nil, err
	}
	for _, item := range columns {
		if strings.EqualFold(item.Name, column) {
			return &ColumnInfo{Dialect: strings.ToLower(dialect.Name), Length: item.Length}, nil
		}
	}
	return nil, fmt.Errorf("column %s.%s is unavailable", table, column)
}
