package bootstrap

import (
	"fmt"

	"github.com/viant/bindly/locator"
	dsql "github.com/viant/datly/sql"
	viewprovider "github.com/viant/datly/sql/reader/provider"
)

// ViewRuntimeConfig supplies the connection registry for independent reads.
type ViewRuntimeConfig struct {
	SQL *dsql.SQLComponent
}

// NewViewProvider attaches execution dependencies to the artifact's compiled
// independent reads without exposing their plans to composition callers.
func (a *Artifact) NewViewProvider(config ViewRuntimeConfig) (locator.Provider, error) {
	if a == nil {
		return nil, fmt.Errorf("compiled component artifact is required")
	}
	if len(a.ViewDependencies) == 0 {
		return nil, nil
	}
	return viewprovider.New(viewprovider.Config{Dependencies: a.ViewDependencies, Input: a.Input, SQL: config.SQL})
}
