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
	target ComponentTarget
	fields func(reflect.Type, string) ([]SelectorFieldName, error)
}
type selectorPresentationKey struct{}

func NewSelectorPresentation(target ComponentTarget, fields func(reflect.Type, string) ([]SelectorFieldName, error)) *SelectorPresentation {
	target.Route.Method = strings.ToUpper(strings.TrimSpace(target.Route.Method))
	target.Route.Path = strings.TrimSpace(target.Route.Path)
	return &SelectorPresentation{target: target, fields: fields}
}

// EnterSelectorPresentation clears inherited metadata at every component boundary.
func EnterSelectorPresentation(ctx context.Context, target ComponentTarget, presentation *SelectorPresentation) (context.Context, error) {
	var value SelectorPresentation
	if presentation != nil {
		if presentation.target != target || presentation.fields == nil {
			return ctx, fmt.Errorf("selector presentation does not match target %s", target.String())
		}
		value = *presentation
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
