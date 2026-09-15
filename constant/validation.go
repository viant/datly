package constant

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly/xform/conv"
	"github.com/viant/datly/spec"
	"github.com/viant/x/shape"
)

// Validate checks external values with the same native conversion used by
// Bindly's const provider, before any discovery/connection work. Authored
// declaration consistency is deliberately left to the DQL compiler.
func (v *Values) Validate(component *spec.Component, lookup func(string) (reflect.Type, error)) error {
	if v.Empty() || component == nil {
		return nil
	}
	for _, p := range spec.EffectiveParameters(component.Parameters) {
		if p == nil || !strings.EqualFold(p.Source.Kind, "const") {
			continue
		}
		name := p.Source.Name
		if name == "" {
			name = p.Name
		}
		value, ok := v.Lookup(name)
		if !ok {
			continue
		}
		expression := p.TypeExpr
		if expression == "" {
			expression = "string"
		}
		typ, err := (shape.Runtime{Lookup: lookup}).Type(expression)
		if err != nil {
			return fmt.Errorf("constant %q type: %w", name, err)
		}
		if typ == nil {
			return fmt.Errorf("constant %q requires a resolved type", name)
		}
		if _, err = (conv.ValueConverter{}).Convert(value, typ); err != nil {
			return fmt.Errorf("constant %q cannot convert to %s", name, expression)
		}
	}
	return nil
}
