package standalone

import (
	"context"
	"fmt"
	"strings"

	sqlconfig "github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/sink"
)

// ColumnInfo is live connector schema metadata for application startup checks.
// A nil Length means the dialect does not declare a character limit.
type ColumnInfo struct {
	Dialect string
	Length  *int64
}

// ConfiguredDriver exposes only the selected connector's static driver name.
// It performs no database/schema query and never returns connection secrets.
func (s *Server) ConfiguredDriver(ctx context.Context, connectorName string) (string, error) {
	if s == nil || s.source == nil || s.source.connections == nil {
		return "", fmt.Errorf("linked connector is required")
	}
	return s.source.connections.ConfiguredDriver(ctx, connectorName)
}

// InspectColumn reads one column through SQLX's metadata service. Application
// callers receive metadata only; no raw database handle or SQL execution API
// crosses the standalone runtime boundary.
func (s *Server) InspectColumn(ctx context.Context, connectorName, table, column string) (*ColumnInfo, error) {
	if strings.TrimSpace(column) == "" {
		return nil, fmt.Errorf("column name is required")
	}
	columns, dialectName, err := s.inspectTableColumns(ctx, connectorName, table)
	if err != nil {
		return nil, err
	}
	for _, item := range columns {
		if strings.EqualFold(item.Name, column) {
			return &ColumnInfo{Dialect: dialectName, Length: item.Length}, nil
		}
	}
	return nil, fmt.Errorf("column %s.%s is unavailable", table, column)
}

// HasTable checks live connector metadata without exposing a DB handle or SQL
// execution surface to application callers.
func (s *Server) HasTable(ctx context.Context, connectorName, table string) (bool, error) {
	columns, _, err := s.inspectTableColumns(ctx, connectorName, table)
	if err != nil {
		return false, err
	}
	return len(columns) > 0, nil
}

func (s *Server) inspectTableColumns(ctx context.Context, connectorName, table string) ([]sink.Column, string, error) {
	if s == nil || s.source == nil || s.source.connections == nil || ctx == nil {
		return nil, "", fmt.Errorf("linked connector and context are required")
	}
	if strings.TrimSpace(table) == "" {
		return nil, "", fmt.Errorf("table name is required")
	}
	connections := s.source.connections
	db, err := connections.ResolveDB(ctx, connectorName)
	if err != nil {
		return nil, "", err
	}
	dialect, err := connections.SQL.Dialect(ctx)
	if err != nil {
		return nil, "", err
	}
	if dialect == nil {
		return nil, "", fmt.Errorf("connector dialect is unavailable")
	}
	session, err := sqlconfig.Session(ctx, db, dialect)
	if err != nil {
		return nil, "", err
	}
	columns, err := sqlconfig.Columns(ctx, session, db, table, dialect)
	if err != nil {
		return nil, "", err
	}
	return columns, strings.ToLower(dialect.Name), nil
}
