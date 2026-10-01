package engine

import (
	"context"
	"fmt"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/structology"
	"reflect"
	"strings"
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
	declared    []registry.InputField
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

func (m *inputReadMetadata) seal() {
	m.mu.Lock()
	defer m.mu.Unlock()
	var state *structology.State
	for _, field := range m.declared {
		binding := field.Binding()
		if binding.When == "" || binding.Location.Kind != "view" {
			continue
		}
		if _, known := m.projections[field.Path()]; known {
			continue
		}
		if state == nil {
			state = structology.NewStateType(reflect.TypeOf(m.target)).WithValue(m.target)
		}
		enabled, err := state.Value(strings.TrimPrefix(binding.When, "$"))
		if active, ok := enabled.(bool); err == nil && ok && !active {
			m.projections[field.Path()] = skippedReadProjection{}
		}
	}
	m.ready = true
}

// A skipped declared read has no row evidence. Keeping its projection identity
// lets empty auxiliary collections be captured; any attempt to use a field as
// loaded (including stale supplied rows) still fails explicitly.
type skippedReadProjection struct{}

func (skippedReadProjection) RootHolder() string { return "" }
func (skippedReadProjection) DirectOutput() bool { return true }
func (skippedReadProjection) Fields(int, ...xhandler.ReadStep) (xhandler.FieldSet, error) {
	return nil, fmt.Errorf("declared read was skipped; no fields were loaded")
}

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
