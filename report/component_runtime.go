package report

import (
	"fmt"

	"github.com/viant/bindly/locator"
	dsql "github.com/viant/datly/sql"
	viewprovider "github.com/viant/datly/sql/reader/provider"
)

// HasLinkedHandler reports whether bootstrap resolved an authored handler factory.
// The handler itself is installed only by canonical artifact registration.
func (a *ComponentArtifact) HasLinkedHandler() bool {
	return a != nil && a.artifact != nil && a.artifact.Handler != nil
}

// NewViewProvider attaches SQL to compiled independent views without exposing
// their plans or moving canonical input binding into the host.
// Components without independent views return nil.
func (a *ComponentArtifact) NewViewProvider(sql *dsql.SQLComponent) (locator.Provider, error) {
	if a == nil || a.artifact == nil {
		return nil, fmt.Errorf("compiled component artifact is required")
	}
	if len(a.artifact.ViewDependencies) == 0 {
		return nil, nil
	}
	return viewprovider.New(viewprovider.Config{Dependencies: a.artifact.ViewDependencies, Input: a.artifact.Input, SQL: sql})
}
