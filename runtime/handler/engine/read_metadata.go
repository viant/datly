package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/viant/bindly"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
)

// inputReadMetadata belongs to one invocation. Only successful assignments to
// its canonical input publish evidence; nested BindTarget values do not.
type inputReadMetadata struct {
	mu          sync.RWMutex
	target      any
	ready       bool
	projections map[string]xhandler.ReadProjection
}

func (m *inputReadMetadata) observe(_ context.Context, event bindly.BindingEvent) error {
	if event.Target != m.target {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ready {
		return fmt.Errorf("input read metadata is sealed")
	}
	// A later successful assignment without evidence clears any earlier value;
	// opaque conversion/defaults cannot accidentally retain old provenance.
	delete(m.projections, event.Path)
	projection, ok := event.Metadata.(xhandler.ReadProjection)
	if ok && !(xshape.Runtime{}).IsNil(projection) {
		m.projections[event.Path] = projection
	}
	return nil
}

func (m *inputReadMetadata) seal() { m.mu.Lock(); m.ready = true; m.mu.Unlock() }
func (m *inputReadMetadata) Projection(inputField string) (xhandler.ReadProjection, error) {
	if m == nil {
		return nil, fmt.Errorf("input read metadata is unavailable")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.ready {
		return nil, fmt.Errorf("input read metadata is unavailable during binding")
	}
	projection, found := m.projections[inputField]
	if !found {
		return nil, fmt.Errorf("input field %s has no known read projection", inputField)
	}
	return projection, nil
}
