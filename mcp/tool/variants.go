package tool

import (
	"fmt"
	"reflect"

	"github.com/viant/mcp-protocol/schema"
)

// PathVariant retains an existing route plan and the arguments activating it.
// Plans are assembled once during publication, never from client route names.
type PathVariant struct {
	Plan  *Plan
	Keys  []Argument
	names [][]string
}

func (p *Plan) AddPathVariant(variant *Plan) error {
	if p == nil || variant == nil {
		return fmt.Errorf("MCP route variant requires compiled plans")
	}
	if p.Target().Component != variant.Target().Component || p.Target().Route.Method != variant.Target().Route.Method {
		return fmt.Errorf("MCP route variant must belong to the same component and method")
	}
	for _, existing := range p.variants {
		if existing.Plan.Target() == variant.Target() {
			return fmt.Errorf("duplicate MCP route variant")
		}
	}

	known := map[string]bool{}
	for _, arg := range p.args {
		known[arg.publicName] = true
	}
	var keys []Argument
	for _, arg := range variant.args {
		if known[arg.publicName] {
			continue
		}
		if arg.sourceKind != "path" {
			return fmt.Errorf("MCP route variant adds non-path argument %q", arg.publicName)
		}
		keys = append(keys, arg)
	}
	if len(keys) == 0 {
		return fmt.Errorf("MCP route variant has no distinguishing identity")
	}
	if _, err := MergePathMetadata(p.Metadata(), variant.Metadata()); err != nil {
		return err
	}
	names := make([][]string, len(keys))
	for i, key := range keys {
		names[i] = append([]string{key.publicName}, key.aliases...)
	}
	p.variants = append(p.variants, PathVariant{Plan: variant, Keys: keys, names: names})
	return nil
}

func (p *Plan) PathVariants() []PathVariant {
	result := append([]PathVariant(nil), p.variants...)
	for i := range result {
		result[i].Keys = append([]Argument(nil), result[i].Keys...)
	}
	return result
}

func (p *Plan) SelectPath(arguments map[string]any) (*Plan, error) {
	selected := p
	best := 0
	for _, variant := range p.variants {
		supplied := 0
		for index := range variant.Keys {
			present := false
			for _, name := range variant.names[index] {
				value, ok := arguments[name]
				if !ok {
					continue
				}
				present = true
				if value == nil {
					return nil, fmt.Errorf("MCP path argument %q cannot be null", name)
				}
				v := reflect.ValueOf(value)
				if (v.Kind() == reflect.String || v.Kind() == reflect.Slice || v.Kind() == reflect.Array) && v.Len() == 0 {
					return nil, fmt.Errorf("MCP path argument %q cannot be empty", name)
				}
			}
			if present {
				supplied++
			}
		}
		if supplied == 0 {
			continue
		}
		if supplied != len(variant.Keys) {
			return nil, fmt.Errorf("MCP route identity is incomplete")
		}
		if supplied == best {
			return nil, fmt.Errorf("MCP route identity is ambiguous")
		}
		if supplied > best {
			selected, best = variant.Plan, supplied
		}
	}
	return selected, nil
}

// MergePathMetadata adds optional path selectors to the collection schema.
// Common bindings must expose identical schemas; route-specific requiredness
// remains enforced by the selected route at invocation.
func MergePathMetadata(base, variant schema.Tool) (schema.Tool, error) {
	for name, property := range variant.InputSchema.Properties {
		if existing, ok := base.InputSchema.Properties[name]; ok {
			if !reflect.DeepEqual(existing, property) {
				return schema.Tool{}, fmt.Errorf("MCP route variants disagree on argument %q", name)
			}
		} else {
			base.InputSchema.Properties[name] = property
		}
	}
	return base, nil
}
