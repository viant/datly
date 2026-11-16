package exec

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
)

// SelectorFieldName identifies one visible field in an exact output row.
// Index is the canonical Go field index, never a SQL expression.
type SelectorFieldName struct {
	GoName     string
	PublicName string
	Index      []int
}

// SelectorPresentation is trusted presentation metadata for one invocation.
// The callback must close over an immutable output plan, never request state.
type SelectorPresentation struct {
	target         ComponentTarget
	fieldSelection spec.FieldSelectionMode
	fields         func(reflect.Type, string) ([]SelectorFieldName, error)
	outputRoot     *SelectorOutputRoot
}
type selectorPresentationKey struct{}

func NewSelectorPresentation(target ComponentTarget, fields func(reflect.Type, string) ([]SelectorFieldName, error)) *SelectorPresentation {
	target.Route.Method = strings.ToUpper(strings.TrimSpace(target.Route.Method))
	target.Route.Path = strings.TrimSpace(target.Route.Path)
	return &SelectorPresentation{target: target, fields: fields}
}

// WithFieldSelection returns detached trusted metadata with an invocation mode.
// The original presentation remains reusable by independent invocations.
func (p *SelectorPresentation) WithFieldSelection(mode spec.FieldSelectionMode) *SelectorPresentation {
	if p == nil {
		return nil
	}
	result := *p
	result.fieldSelection = mode
	return &result
}

// SelectorOutputRoot binds response-only root Fields to declared public storage.
// Resolve closes over the immutable output plan's indexed field authority.
type SelectorOutputRoot struct {
	View       string
	Path       []string
	RowType    reflect.Type
	OutputType reflect.Type
	Resolve    func(string) (SelectorFieldName, bool, error)
}

func (r *SelectorOutputRoot) clone() *SelectorOutputRoot {
	if r == nil {
		return nil
	}
	result := *r
	result.Path = append([]string(nil), r.Path...)
	return &result
}

// WithOutputRoot returns detached metadata; no caller rows or selection are retained.
func (p *SelectorPresentation) WithOutputRoot(root *SelectorOutputRoot) *SelectorPresentation {
	if p == nil {
		return nil
	}
	result := *p
	result.outputRoot = root.clone()
	return &result
}

// PresentedSelectorOutputRoot is available only inside its exact component entry.
func PresentedSelectorOutputRoot(ctx context.Context, component spec.Key) *SelectorOutputRoot {
	value, _ := ctx.Value(selectorPresentationKey{}).(SelectorPresentation)
	if value.fields == nil || value.target.Component != component {
		return nil
	}
	return value.outputRoot.clone()
}

// EnterSelectorPresentation clears inherited metadata at every component boundary.
func EnterSelectorPresentation(ctx context.Context, target ComponentTarget, presentation *SelectorPresentation) (context.Context, error) {
	var value SelectorPresentation
	if presentation != nil {
		if presentation.target != target || presentation.fields == nil {
			return ctx, fmt.Errorf("selector presentation does not match target %s", target.String())
		}
		if err := presentation.fieldSelection.Validate(); err != nil {
			return ctx, err
		}
		if root := presentation.outputRoot; root != nil {
			if presentation.fieldSelection != spec.FieldSelectionOutputOnly || root.View == "" || len(root.Path) != 1 || root.Path[0] == "" || root.OutputType == nil || root.RowType == nil || root.RowType.Kind() != reflect.Struct || root.Resolve == nil {
				return ctx, fmt.Errorf("selector public output root requires root-only outputOnly metadata")
			}
		}
		value = *presentation
		value.outputRoot = presentation.outputRoot.clone()
	}
	return context.WithValue(ctx, selectorPresentationKey{}, value), nil
}

// PresentedSelectorFields returns detached metadata only to its component owner.
func PresentedSelectorFields(ctx context.Context, component spec.Key, rowType reflect.Type, path string) ([]SelectorFieldName, error) {
	value, _ := ctx.Value(selectorPresentationKey{}).(SelectorPresentation)
	if value.fields == nil || value.target.Component != component {
		return nil, nil
	}
	fields, err := value.fields(rowType, path)
	if err != nil {
		return nil, err
	}
	result := make([]SelectorFieldName, len(fields))
	for i, field := range fields {
		result[i] = field
		result[i].Index = append([]int(nil), field.Index...)
	}
	return result, nil
}

// PresentedSelectorFieldSelection returns the trusted invocation mode only to its owner.
func PresentedSelectorFieldSelection(ctx context.Context, component spec.Key) spec.FieldSelectionMode {
	value, _ := ctx.Value(selectorPresentationKey{}).(SelectorPresentation)
	if value.fields == nil || value.target.Component != component {
		return ""
	}
	return value.fieldSelection
}
