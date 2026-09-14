package bootstrap

import (
	"context"
	"fmt"
	"github.com/viant/datly/runtime/jobs"
	dsql "github.com/viant/datly/sql"
)

// JobStoreConfig resolves job persistence through the same configured connector
// owner used by registered SQL components. Jobs retain only the selected native
// database/dialect pair, not application SQL execution or a connector registry.
type JobStoreConfig struct {
	SQL       *dsql.SQLComponent
	Connector string
	Table     string
	Dataset   string
	// DisableTableCreation requires a pre-provisioned original job table.
	// The default mirrors original job-service initialization.
	DisableTableCreation bool
}

func (c JobStoreConfig) NewStore(ctx context.Context) (*jobs.SQLStore, error) {
	if c.SQL == nil {
		return nil, fmt.Errorf("job SQL connector configuration is required")
	}
	connection, err := c.SQL.Resolve(ctx, c.Connector)
	if err != nil {
		return nil, err
	}
	return jobs.NewSQLStore(ctx, jobs.SQLConfig{DB: connection.DB, Dialect: connection.Dialect, Table: c.Table, Dataset: c.Dataset, DisableTableCreation: c.DisableTableCreation})
}
