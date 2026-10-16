package sql

import (
	"fmt"
	"strings"

	"github.com/viant/sqlx/metadata/registry"
)

// IdentifierProduct uses driver metadata only: no connection, query, transaction,
// or dialect discovery is performed during component compilation.
func (c *SQLComponent) IdentifierProduct(connector string) (string, error) {
	if c == nil {
		return "", fmt.Errorf("sql component is required")
	}
	connector = strings.TrimSpace(connector)
	c.mu.RLock()
	db := c.DB
	named := c.connectors[connector]
	hasNamed := len(c.connectors) != 0
	if named != nil {
		db = named.db
	}
	c.mu.RUnlock()
	if named == nil && connector != "" && hasNamed {
		return "", fmt.Errorf("sql connector %q is not registered", connector)
	}
	if db == nil {
		return "", fmt.Errorf("sql connector %q db is required", connector)
	}
	if product := registry.MatchProduct(db); product != nil {
		return product.Name, nil
	}
	return "", nil
}
