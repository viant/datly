package registry

import (
	"reflect"
	"strings"
	"time"

	"github.com/viant/datly/spec"
	"github.com/viant/tagly/format"
)

// TimeInputSchema describes an explicitly declared native body time policy.
// Output continues to use the encoder's narrower time layout contract.
func TimeInputSchema(field reflect.StructField) (*spec.WireSchema, error) {
	base := field.Type
	for base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	if base != reflect.TypeFor[time.Time]() {
		return nil, nil
	}
	policy, err := format.Parse(field.Tag)
	if err != nil {
		return nil, err
	}
	if !policy.HasTimeInputPolicy() {
		return nil, nil
	}
	result := &spec.WireSchema{Nullable: field.Type.Kind() == reflect.Pointer}
	stringsSchema := &spec.WireSchema{Type: "string"}
	layouts := strings.Split(policy.TimeInputLayouts, "|")
	patterns := make([]string, 0, len(layouts)+1)
	complete := policy.TimeInputLayouts != ""
	for _, layout := range layouts {
		pattern := ""
		switch layout {
		case time.RFC3339, time.RFC3339Nano:
			pattern = `[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.,][0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})`
		case "2006-01-02T15:04:05":
			pattern = `[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.,][0-9]+)?`
		case "2006-01-02 15:04:05":
			pattern = `[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.,][0-9]+)?`
		case "2006-01-02":
			pattern = `[0-9]{4}-[0-9]{2}-[0-9]{2}`
		default:
			complete = false
		}
		patterns = append(patterns, pattern)
	}
	if policy.TimeEmpty == "preserve" {
		patterns = append(patterns, "")
	}
	if complete {
		stringsSchema.Pattern = "^(?:" + strings.Join(patterns, "|") + ")$"
	}
	result.AnyOf = append(result.AnyOf, stringsSchema)
	if policy.TimeEpoch == "milliseconds" {
		result.AnyOf = append(result.AnyOf, &spec.WireSchema{Type: "integer"})
	}
	return result, nil
}
