package generate

import (
	"reflect"
	"strings"

	"github.com/viant/tagly/format/text"
	"github.com/viant/tagly/tags"
)

// writerScalarJSONTag carries the component's authored presentation policy into
// generated structs, so encoding/json and the runtime encoder agree.
func writerScalarJSONTag(plan *Plan, name, raw string) string {
	if plan == nil {
		return raw
	}
	omitEmpty := strings.TrimSpace(plan.Settings.Mutation) != "" && plan.Generation != nil && plan.Generation.WriterOmitEmpty
	parsed := tags.NewTags(raw)
	jsonValue, explicit := reflect.StructTag(raw).Lookup("json")
	parts := strings.Split(jsonValue, ",")
	if len(parts) > 1 {
		clean := parts[:1]
		for _, option := range parts[1:] {
			if option != "" {
				clean = append(clean, option)
			}
		}
		parts = clean
	}
	if explicit && parts[0] != "" {
		return raw
	}
	if !explicit && reflect.StructTag(raw).Get("internal") == "true" {
		return raw
	}
	format := text.NewCaseFormat(plan.Settings.CaseFormat)
	if format != text.CaseFormatUndefined {
		parts[0] = text.DetectCaseFormat(name).Format(name, format)
	} else if !explicit && !omitEmpty {
		return raw
	}
	if !explicit && omitEmpty {
		parts = append(parts, "omitempty")
	}
	parsed.Set("json", strings.Join(parts, ","))
	return parsed.Stringify()
}
