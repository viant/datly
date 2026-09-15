package report

import (
	"fmt"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
)

// HasLinkedHandler reports whether bootstrap resolved an authored handler factory.
// The handler itself is installed only by canonical artifact registration.
func (a *ComponentArtifact) HasLinkedHandler() bool {
	return a != nil && a.artifact != nil && a.artifact.Handler != nil
}

// NewViewProvider attaches SQL to compiled independent views without exposing
// their plans or moving canonical input binding into the host.
// Components without independent views return nil.
func (a *ComponentArtifact) NewViewProvider(config bootstrap.ViewRuntimeConfig) (locator.Provider, error) {
	if a == nil || a.artifact == nil {
		return nil, fmt.Errorf("compiled component artifact is required")
	}
	return a.artifact.NewViewProvider(config)
}
