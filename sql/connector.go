package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/viant/xdatly/connector"
)

// Connector is the explicitly opt-in handler provider. Unlike reader Resolve,
// it never falls back to DB or initializes dialect/transaction state.
func (c *SQLComponent) Connector(ctx context.Context, name string) (*sql.DB, error) {
	if c == nil || ctx == nil {
		return nil, fmt.Errorf("connector provider and context are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("connector name is required")
	}
	c.mu.RLock()
	connection := c.connectors[name]
	c.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if connection == nil || connection.db == nil {
		return nil, fmt.Errorf("sql connector %q is not registered", name)
	}
	return connection.db, nil
}

var _ connector.Provider = (*SQLComponent)(nil)
