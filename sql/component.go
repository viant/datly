package sql

import (
	"context"
	dsql "database/sql"
	"fmt"
	"strings"
	"sync"

	sqlconfig "github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/info"
)

// SQLComponent binds a registered component to its default and named SQL
// sources. Connector registration is bootstrap state; dialects are resolved
// lazily and cached with their database handle.
type SQLComponent struct {
	DB *dsql.DB
	// Tx is a caller-owned transaction for reader execution against DB.
	// Named connectors must resolve to the same DB while Tx is set.
	Tx *dsql.Tx

	mu         sync.RWMutex
	dialect    *info.Dialect
	connectors map[string]*connection
}

type connection struct {
	db *dsql.DB

	mu      sync.Mutex
	dialect *info.Dialect
}

// Connection is the DB and dialect pair selected for one view source.
type Connection struct {
	DB      *dsql.DB
	Tx      *dsql.Tx
	Dialect *info.Dialect
}

// RegisterConnector binds an authored connector name to its database handle.
// Named connectors are bootstrap configuration and must be registered before
// the component serves requests.
func (c *SQLComponent) RegisterConnector(name string, db *dsql.DB) error {
	if c == nil {
		return fmt.Errorf("sql component is required")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("connector name is required")
	}
	if db == nil {
		return fmt.Errorf("connector %s db is required", name)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connectors == nil {
		c.connectors = map[string]*connection{}
	}
	c.connectors[name] = &connection{db: db}
	return nil
}

// Resolve returns the DB and dialect selected by immutable view connector
// metadata. A component with no named registrations uses DB as its sole
// source, including when metadata gives that single source a name.
func (c *SQLComponent) Resolve(ctx context.Context, connector string) (Connection, error) {
	if c == nil {
		return Connection{}, fmt.Errorf("sql component is required")
	}
	connector = strings.TrimSpace(connector)
	c.mu.RLock()
	named := c.connectors[connector]
	hasNamed := len(c.connectors) > 0
	defaultDB := c.DB
	c.mu.RUnlock()
	if named != nil {
		if c.Tx != nil && named.db != defaultDB {
			return Connection{}, fmt.Errorf("transactional reader connector %s must use the transaction database", connector)
		}
		dialect, err := named.resolveDialect(ctx)
		if err != nil {
			return Connection{}, fmt.Errorf("resolve connector %s dialect: %w", connector, err)
		}
		return Connection{DB: named.db, Tx: c.Tx, Dialect: dialect}, nil
	}
	if connector != "" && hasNamed {
		return Connection{}, fmt.Errorf("sql connector %s is not registered", connector)
	}
	if defaultDB == nil {
		return Connection{}, fmt.Errorf("sql component db is required")
	}
	dialect, err := c.Dialect(ctx)
	if err != nil {
		return Connection{}, err
	}
	return Connection{DB: defaultDB, Tx: c.Tx, Dialect: dialect}, nil
}

func (c *SQLComponent) Dialect(ctx context.Context) (*info.Dialect, error) {
	if c == nil || c.DB == nil {
		return nil, fmt.Errorf("sql component db is required")
	}
	c.mu.RLock()
	dialect := c.dialect
	c.mu.RUnlock()
	if dialect != nil {
		return dialect, nil
	}
	dialect, err := sqlconfig.Dialect(ctx, c.DB)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.dialect == nil {
		c.dialect = dialect
	}
	dialect = c.dialect
	c.mu.Unlock()
	return dialect, nil
}

func (c *connection) resolveDialect(ctx context.Context) (*info.Dialect, error) {
	if c == nil || c.db == nil {
		return nil, fmt.Errorf("connector db is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dialect != nil {
		return c.dialect, nil
	}
	dialect, err := sqlconfig.Dialect(ctx, c.db)
	if err != nil {
		return nil, err
	}
	c.dialect = dialect
	return dialect, nil
}
