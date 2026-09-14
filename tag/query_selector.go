package tag

import (
	"fmt"
	"strings"
)

const QuerySelectorName = "querySelector"

// QuerySelector identifies the view property bound to one selector input field.
type QuerySelector struct {
	View string
}

// Value formats query-selector metadata using ParseQuerySelector's grammar.
func (q QuerySelector) Value() (string, error) {
	view := strings.TrimSpace(q.View)
	if view == "" {
		return "", fmt.Errorf("query selector view is required")
	}
	return "view=" + encodeScalarValue(view), nil
}

// ParseQuerySelector accepts querySelector:"users" and
// querySelector:"view=users".
func ParseQuerySelector(value string) (*QuerySelector, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, fmt.Errorf("query selector view is required")
	}
	if key, mapped, ok := strings.Cut(value, "="); ok {
		if !strings.EqualFold(strings.TrimSpace(key), "view") {
			return nil, fmt.Errorf("unsupported query selector option %q", key)
		}
		value = strings.TrimSpace(mapped)
	}
	var err error
	value, err = decodeScalarValue(value)
	if err != nil {
		return nil, err
	}
	if value == "" {
		return nil, fmt.Errorf("query selector view is required")
	}
	return &QuerySelector{View: value}, nil
}
