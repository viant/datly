package compiler

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema/jsonschema"
	xshape "github.com/viant/x/shape"
)

// compileWireTypes resolves discovery authority using the same component lookup
// as codecs. The source is the effective binding's source, never the representation.
func (c *Compiler) compileWireTypes(bindings []bindly.BindingSpec) (map[string]reflect.Type, error) {
	var result map[string]reflect.Type
	linked := xshape.Linked(c.input.InputType)
	for _, binding := range bindings {
		field, err := linked.StructField(binding.Path)
		if err != nil {
			return nil, err
		}
		expressions := []string{strings.TrimSpace(field.Tag.Get("wireType"))}
		if param, ok := binding.Extension.(*spec.Parameter); ok && param != nil {
			expressions = append(expressions, strings.TrimSpace(param.WireTypeExpr), strings.TrimSpace(reflect.StructTag(param.Tag).Get("wireType")))
		}
		var wire reflect.Type
		seen := map[string]bool{}
		for _, expression := range expressions {
			if expression == "" || seen[expression] {
				continue
			}
			seen[expression] = true
			// Canonical Go shape expressions also support source-authored runtime
			// structs. They carry the same trusted wire authority as linked names
			// without requiring a compiled application type for every shape.
			named, nameErr := (xshape.Resolver{}).Named(expression)
			var resolved reflect.Type
			var err error
			if nameErr != nil || named != expression {
				resolved, err = (xshape.Runtime{Lookup: c.input.TypeLookup}).Type(expression)
				if err != nil || resolved == nil || resolved.Kind() != reflect.Struct {
					return nil, fmt.Errorf("input %s field %s wireType %q requires a linked non-pointer named representation or authored struct shape", c.input.InputType, binding.Path, expression)
				}
			} else {
				if c.input.TypeLookup == nil {
					return nil, fmt.Errorf("input %s field %s wireType %q requires component type lookup", c.input.InputType, binding.Path, expression)
				}
				resolved, err = c.input.TypeLookup(expression)
				if err != nil {
					return nil, fmt.Errorf("input %s field %s wireType %q: %w", c.input.InputType, binding.Path, expression, err)
				}
				if resolved == nil || resolved.Kind() == reflect.Pointer || (resolved.Kind() != reflect.Struct && (resolved.Name() == "" || resolved.PkgPath() == "")) {
					return nil, fmt.Errorf("input %s field %s wireType %q requires a linked non-pointer named representation", c.input.InputType, binding.Path, expression)
				}
			}
			if wire != nil && wire != resolved {
				return nil, fmt.Errorf("input %s field %s has conflicting wireType declarations", c.input.InputType, binding.Path)
			}
			wire = resolved
		}
		if wire == nil {
			continue
		}
		anonymous := strings.EqualFold(strings.TrimSpace(field.Tag.Get("anonymous")), "true")
		if param, ok := binding.Extension.(*spec.Parameter); ok && param != nil {
			anonymous = anonymous || strings.EqualFold(strings.TrimSpace(reflect.StructTag(param.Tag).Get("anonymous")), "true")
		}
		if binding.Location.Kind != "body" || anonymous || strings.Contains(binding.Path, ".") {
			return nil, fmt.Errorf("input %s field %s wireType supports only a direct named body argument", c.input.InputType, binding.Path)
		}
		source := binding.SourceType
		if source == nil {
			source = field.Type
		}
		source = (xshape.Runtime{}).Indirect(source)
		if _, err := (jsonschema.Reflector{WireTypes: map[reflect.Type]reflect.Type{source: wire}, ExcludeInternal: true, ExcludeField: func(field reflect.StructField) bool {
			return strings.TrimSpace(field.Tag.Get("mcp")) == "-"
		}}).Compile(jsonschema.Request{Type: source}); err != nil {
			return nil, fmt.Errorf("input %s field %s wireType: %w", c.input.InputType, binding.Path, err)
		}
		if result == nil {
			result = map[string]reflect.Type{}
		}
		result[binding.Path] = wire
	}
	return result, nil
}
