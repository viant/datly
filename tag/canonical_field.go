package tag

import (
	"github.com/viant/tagly/tags"
	"reflect"
	"strings"
)

// CanonicalFieldTag discards obsolete metadata that has no canonical authority.
// Supported field tags retain their values and order for exact contract checks.
func CanonicalFieldTag(raw string) string {
	structTag := reflect.StructTag(raw)
	_, table := structTag.Lookup("docTable")
	_, column := structTag.Lookup("docColumn")
	if !table && !column {
		return raw
	}
	parsed := tags.NewTags(strings.TrimSpace(raw))
	filtered := parsed[:0]
	for _, item := range parsed {
		if item != nil && item.Name != "docTable" && item.Name != "docColumn" {
			filtered = append(filtered, item)
		}
	}
	return filtered.Stringify()
}
