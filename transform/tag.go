package transform

import (
	"fmt"
	"reflect"
	"strings"
)

// Tag is one original Datly transfer declaration, with an exact source path
// and optional named codec supplied by the caller.
type Tag struct {
	From, To, Codec string
}

// Tags reads transfer tags from exported destination fields, including nested
// structs. Cyclic destinations are visited once along each path.
func Tags(destination reflect.Type) ([]Tag, error) {
	for destination != nil && destination.Kind() == reflect.Pointer {
		destination = destination.Elem()
	}
	if destination == nil || destination.Kind() != reflect.Struct {
		return nil, fmt.Errorf("transfer destination must be a struct")
	}
	var result []Tag
	var walk func(reflect.Type, string, map[reflect.Type]bool) error
	walk = func(current reflect.Type, prefix string, visiting map[reflect.Type]bool) error {
		if visiting[current] {
			return nil
		}
		visiting[current] = true
		defer delete(visiting, current)
		for i := 0; i < current.NumField(); i++ {
			field := current.Field(i)
			if !field.IsExported() {
				continue
			}
			path := prefix + field.Name
			if raw, ok := field.Tag.Lookup("transfer"); ok {
				tag, err := parseTag(raw)
				if err != nil {
					return fmt.Errorf("transfer field %s: %w", path, err)
				}
				tag.To = path
				result = append(result, tag)
				continue
			}
			child := field.Type
			for child.Kind() == reflect.Pointer {
				child = child.Elem()
			}
			if child.Kind() == reflect.Struct {
				if err := walk(child, path+".", visiting); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(destination, "", map[reflect.Type]bool{}); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("transfer destination %s has no transfer tags", destination)
	}
	return result, nil
}

func parseTag(raw string) (Tag, error) {
	var result Tag
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return Tag{}, fmt.Errorf("empty tag item")
		}
		key, value, paired := strings.Cut(part, "=")
		if !paired {
			if result.From != "" {
				return Tag{}, fmt.Errorf("source path declared more than once")
			}
			result.From = part
			continue
		}
		value = strings.TrimSpace(value)
		if value == "" || strings.Contains(value, "=") {
			return Tag{}, fmt.Errorf("invalid tag item %q", part)
		}
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "from":
			if result.From != "" {
				return Tag{}, fmt.Errorf("source path declared more than once")
			}
			result.From = value
		case "codec":
			if result.Codec != "" {
				return Tag{}, fmt.Errorf("codec declared more than once")
			}
			result.Codec = value
		default:
			return Tag{}, fmt.Errorf("unknown tag option %q", key)
		}
	}
	if result.From == "" {
		return Tag{}, fmt.Errorf("source path is required")
	}
	return result, nil
}
