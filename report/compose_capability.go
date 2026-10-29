package report

import (
	"fmt"
	"reflect"

	"github.com/viant/datly/exec"
	"github.com/viant/datly/report/cubecompose"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

// CompositionDefinition is trusted application metadata to prepare against an
// existing native compose handler. Preparation grants no execution authority.
type CompositionDefinition struct {
	SQL         string
	FrameCount  int
	ResultLimit *int
}

// CompositionFilter describes one generated filter and its original input.
// Type is the source Go type; native binding retains its wire conversion rules.
type CompositionFilter struct {
	Name      string
	Parameter string
	Type      reflect.Type
	Required  bool
}

type CompositionBudget struct {
	MaxFrames int
	MaxRows   int
	TimeoutMs int
}

// PreparedComposition contains detached metadata only. The executable native
// plan, catalog, binder and input holders are never exposed.
type PreparedComposition struct {
	source  exec.ComponentTarget
	columns []cubecompose.Column
	filters []CompositionFilter
	budget  CompositionBudget
}

func (p *PreparedComposition) SourceTarget() exec.ComponentTarget { return p.source }
func (p *PreparedComposition) Columns() []cubecompose.Column {
	return append([]cubecompose.Column(nil), p.columns...)
}
func (p *PreparedComposition) Filters() []CompositionFilter {
	return append([]CompositionFilter(nil), p.filters...)
}
func (p *PreparedComposition) Budget() CompositionBudget { return p.budget }

// CompositionSource recognizes only Datly's native derived compose handler.
// Unknown wrappers are rejected rather than unwrapped or trusted by shape.
func CompositionSource(handler rhandler.Handler) (exec.ComponentTarget, bool) {
	h, ok := handler.(*composeHandler)
	if !ok || h == nil || h.source == nil {
		return exec.ComponentTarget{}, false
	}
	return h.source.target, true
}

// CompositionMatchesSource additionally proves the originating compiled input
// contract. Equal names/routes from a different compilation are insufficient.
func CompositionMatchesSource(handler rhandler.Handler, source *registry.RegisteredComponent) bool {
	h, ok := handler.(*composeHandler)
	if !ok || h == nil || h.source == nil || source == nil || source.Component == nil || source.Input == nil || source.Component.Key != h.source.target.Component {
		return false
	}
	contract, found := source.Input.ForRoute(h.source.target.Route)
	return found && contract == h.contract
}

// PrepareComposition reuses the exact catalog, budgets and compiler used by
// execution. It does not invoke a source, bind credentials or execute SQL.
func PrepareComposition(handler rhandler.Handler, definition CompositionDefinition) (*PreparedComposition, error) {
	h, ok := handler.(*composeHandler)
	if !ok || h == nil || h.source == nil || h.config == nil || h.metadata == nil {
		return nil, fmt.Errorf("native compose handler required")
	}
	plan, err := h.compileComposition(definition.SQL, definition.FrameCount, definition.ResultLimit)
	if err != nil {
		return nil, err
	}
	result := &PreparedComposition{
		source: h.source.target, columns: append([]cubecompose.Column(nil), plan.Columns...),
		budget: CompositionBudget{MaxFrames: h.config.MaxCubes, MaxRows: h.config.MaxLimit, TimeoutMs: h.config.TimeoutMs},
	}
	for _, filter := range h.metadata.filters {
		parameter, ok := filter.contract.Binding().Extension.(*spec.Parameter)
		if !ok || parameter == nil {
			return nil, fmt.Errorf("compose filter source parameter unavailable")
		}
		result.filters = append(result.filters, CompositionFilter{
			Name: selectionWireName(filter.field), Parameter: parameter.Name,
			Type: filterSourceType(filter), Required: parameter.Required != nil && *parameter.Required,
		})
	}
	return result, nil
}

func (h *composeHandler) compileComposition(SQL string, frames int, resultLimit *int) (*cubecompose.Plan, error) {
	if frames < 1 || frames > h.config.MaxCubes {
		return nil, fmt.Errorf("cube compose requires between 1 and %d cubes", h.config.MaxCubes)
	}
	return cubecompose.CompileWithResultLimit(SQL, h.catalog, frames, h.config.MaxLimit, resultLimit)
}
