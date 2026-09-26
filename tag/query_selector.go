package tag

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	tagly "github.com/viant/tagly/tags"
)

const QuerySelectorName = "querySelector"

// QuerySelector identifies the view property bound to one selector input field.
type QuerySelector struct {
	View     string
	Property spec.SelectorProperty
}

// Value formats query-selector metadata using ParseQuerySelector's grammar.
func (q QuerySelector) Value() (string, error) {
	view := strings.TrimSpace(q.View)
	if view == "" {
		return "", fmt.Errorf("query selector view is required")
	}
	result := "view=" + encodeScalarValue(view)
	if q.Property != "" {
		property, ok := spec.SelectorPropertyForParam(string(q.Property))
		if !ok {
			return "", fmt.Errorf("unsupported query selector property %q", q.Property)
		}
		result += ",property=" + string(property)
	}
	return result, nil
}

// ParseQuerySelector accepts querySelector:"users" and
// querySelector:"view=users".
func ParseQuerySelector(value string) (*QuerySelector, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, fmt.Errorf("query selector view is required")
	}
	if !strings.Contains(value, "=") {
		view, err := decodeScalarValue(value)
		if err != nil {
			return nil, err
		}
		if view == "" {
			return nil, fmt.Errorf("query selector view is required")
		}
		return &QuerySelector{View: view}, nil
	}
	result := &QuerySelector{}
	seen := map[string]bool{}
	err := tagly.Values(value).MatchRawPairs(func(key, value string) error {
		key = strings.ToLower(strings.TrimSpace(key))
		if seen[key] {
			return fmt.Errorf("duplicate query selector option %q", key)
		}
		seen[key] = true
		decoded, err := decodeScalarValue(strings.TrimSpace(value))
		if err != nil {
			return err
		}
		switch key {
		case "view":
			result.View = decoded
		case "property":
			property, ok := spec.SelectorPropertyForParam(decoded)
			if !ok {
				return fmt.Errorf("unsupported query selector property %q", decoded)
			}
			result.Property = property
		default:
			return fmt.Errorf("unsupported query selector option %q", key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if result.View == "" {
		return nil, fmt.Errorf("query selector view is required")
	}
	return result, nil
}
