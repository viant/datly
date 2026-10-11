package template

import (
	"fmt"
	"sort"

	"github.com/viant/datly/constant/identifier"
)

// constantBindings contains only validated, trusted values. Native Velty emits
// them during SQL evaluation; authored and compiled SQL never contain the values.
type constantBindings struct{ values []string }

func (c *constantBindings) Get(index int) (string, error) {
	if c == nil || index < 0 || index >= len(c.values) {
		return "", fmt.Errorf("invalid SQL constant slot")
	}
	return c.values[index], nil
}

// ConstantRenderer retains the SQL compiler facade over the shared immutable
// identifier renderer.
type ConstantRenderer identifier.Renderer

func (r ConstantRenderer) plan(source string) ([]sourceReplacement, error) {
	references, err := (identifier.Renderer)(r).References(source)
	if err != nil {
		return nil, err
	}
	if references == nil {
		return nil, nil
	}
	result := make([]sourceReplacement, len(references))
	for i, ref := range references {
		result[i] = sourceReplacement{start: ref.Start, end: ref.End, value: ref.Value}
	}
	return result, nil
}
func (r ConstantRenderer) render(source string) (string, error) {
	return (identifier.Renderer)(r).Render(source)
}
func (r ConstantRenderer) Validate(source string) error {
	return (identifier.Renderer)(r).Validate(source)
}
func (r ConstantRenderer) Identifier(table string) (string, error) {
	return (identifier.Renderer)(r).Identifier(table)
}

func (c Compiler) constantRenderer() ConstantRenderer {
	names := append([]string(nil), c.NonWindowAliases...)
	for _, variable := range c.Variables {
		names = append(names, variable.Name)
	}
	return ConstantRenderer{Values: c.Const, Variables: names}
}

// constantSource compiles reference locations into native template slots. It
// changes only a private compilation copy, and never substitutes actual values.
func (c Compiler) constantSource() (string, *constantBindings, error) {
	references, err := c.constantRenderer().plan(c.Source)
	if err != nil {
		return "", nil, err
	}
	if len(references) == 0 {
		return c.Source, nil, nil
	}
	sort.Slice(references, func(i, j int) bool { return references[i].start > references[j].start })
	bindings := &constantBindings{values: make([]string, len(references))}
	source := c.Source
	for index, reference := range references {
		bindings.values[index] = reference.value
		slot := fmt.Sprintf("${%s.Get(%d)}", constantVariable, index)
		source = source[:reference.start] + slot + source[reference.end:]
	}
	return source, bindings, nil
}
