// Package docs resolves shared annotations without changing executable
// contracts. Original Datly view/state/docs.go owns the source behavior: ordered
// shallow dictionary replacement, table/column fallback, holders and examples.
package docs

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"strings"
)

type dictionary map[string]entry
type entry struct {
	text   string
	object dictionary
	scalar bool
}

func (d dictionary) decode(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: expected documentation dictionary", node.Line)
	}
	for i := 0; i < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		if key.Tag != "!!str" || strings.TrimSpace(key.Value) == "" {
			return fmt.Errorf("line %d: expected nonempty string key", key.Line)
		}
		name := strings.ToLower(key.Value)
		if _, ok := d[name]; ok {
			return fmt.Errorf("line %d: duplicate/case-conflicting key %q", key.Line, key.Value)
		}
		item := entry{}
		switch {
		case value.Kind == yaml.MappingNode:
			item.object = dictionary{}
			if err := item.object.decode(value); err != nil {
				return err
			}
		case value.Kind == yaml.ScalarNode && value.Tag == "!!str":
			item.text = value.Value
			item.scalar = true
		default:
			return fmt.Errorf("line %d: documentation %q requires string or dictionary", value.Line, key.Value)
		}
		d[name] = item
	}
	return nil
}
func (d dictionary) merge(from dictionary) {
	for key, value := range from {
		d[key] = value
	}
}
func (d dictionary) byName(name string) (string, bool) {
	item, ok := d[strings.ToLower(name)]
	if !ok {
		return "", false
	}
	if item.scalar {
		return item.text, true
	}
	return item.object.byName("_")
}
func (d dictionary) column(table, column string) (string, bool) {
	if item, ok := d[strings.ToLower(table)]; ok && item.object != nil {
		if text, ok := item.object.byName(column); ok {
			return text, true
		}
	}
	if table != "" {
		if text, ok := d.byName(table + "." + column); ok {
			return text, true
		}
	}
	return d.byName(column)
}
