package generate

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/tag"
	"github.com/viant/tagly/format/text"
	"github.com/viant/tagly/tags"
)

// applyInferredJSONTags needs the complete struct, including relation holders.
// Case formatting can collapse distinct Go names; preserve their original tags
// so projection can still select canonical fields before runtime formatting.
func applyInferredJSONTags(plan *Plan, fields []Field) error {
	original := make([]string, len(fields))
	for i := range fields {
		field := &fields[i]
		original[i] = field.Tag
		if field.Anonymous || hasStructTag(field.Tag, tag.SelfName) {
			continue
		}
		if field.RelationHolder {
			if format := text.NewCaseFormat(plan.Settings.CaseFormat); format != text.CaseFormatUndefined && !hasStructTag(field.Tag, "json") {
				field.Tag = appendStructTag(field.Tag, "json", text.DetectCaseFormat(field.Name).Format(field.Name, format))
			}
		} else {
			field.Tag = writerScalarJSONTag(plan, field.Name, field.Tag)
		}
	}
	// Restoring a Go name can collide with another inferred name. Repeat until
	// no further restoration is possible, then reject unresolved conflicts.
	for {
		byName := map[string][]int{}
		for i, field := range fields {
			if field.Anonymous {
				continue
			}
			name := strings.Split(reflect.StructTag(field.Tag).Get("json"), ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			byName[name] = append(byName[name], i)
		}
		restored := false
		for _, indexes := range byName {
			if len(indexes) < 2 {
				continue
			}
			for _, i := range indexes {
				if fields[i].Tag != original[i] {
					fields[i].Tag = original[i]
					restored = true
				}
			}
		}
		if restored {
			continue
		}
		for _, field := range fields {
			name := strings.Split(reflect.StructTag(field.Tag).Get("json"), ",")[0]
			if name == "" {
				name = field.Name
			}
			if indexes := byName[name]; len(indexes) > 1 {
				return fmt.Errorf("JSON name %q conflicts between generated fields %q and %q; declare distinct explicit JSON names", name, fields[indexes[0]].Name, fields[indexes[1]].Name)
			}
		}
		return nil
	}
}

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
