package tags

import "strings"

const QuerySelectorTag = "querySelector"

// ParseQuerySelector returns the target view alias encoded in querySelector tag.
// Supported forms:
//
//	querySelector:"vendor"
//	querySelector:"view=vendor"
func ParseQuerySelector(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if key, mapped, ok := strings.Cut(value, "="); ok {
		if strings.EqualFold(strings.TrimSpace(key), "view") {
			return strings.TrimSpace(mapped)
		}
	}
	return value
}
