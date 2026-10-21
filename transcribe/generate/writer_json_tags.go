package generate

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/tagly/format/text"
	"github.com/viant/tagly/tags"
)

// applyInferredJSONTags needs the complete struct, including relation holders.
// Case formatting can collapse distinct Go names; preserve their original tags
// so projection can still select canonical fields before runtime formatting.
func applyInferredJSONTags(plan *Plan, fields []Field, source ...*spec.View) error {
	return applyJSONTags(plan, fields, false, source...)
}

// Body owners leave inferred names to native caseFormat; authored tags remain authoritative.
func applyBodyJSONTags(plan *Plan, fields []Field, source *spec.View) error {
	return applyJSONTags(plan, fields, true, source)
}

func applyJSONTags(plan *Plan, fields []Field, body bool, source ...*spec.View) error {
	original := make([]string, len(fields))
	for i := range fields {
		field := &fields[i]
		original[i] = field.Tag
		if field.Anonymous || hasStructTag(field.Tag, tag.SelfName) {
			continue
		}
		if field.RelationHolder {
			if format := text.NewCaseFormat(plan.Settings.CaseFormat); !body && format != text.CaseFormatUndefined && !hasStructTag(field.Tag, "json") {
				field.Tag = appendStructTag(field.Tag, "json", text.DetectCaseFormat(field.Name).Format(field.Name, format))
			}
		} else if body {
			field.Tag = bodyScalarJSONTag(plan, field.Tag)
		} else if !authoredEmptyJSONName(source, field.Name) {
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
	return parsed.Literal()
}

// Preserve exact option-only JSON authority from the existing canonical owner.
// Use the component's caseFormat for response casing. Filling an authored
// json:",omitempty" name with an inferred name seals that casing into the Go
// shape instead of leaving presentation to caseFormat. Keep the empty name and
// options intact; direct encoding/json callers then retain the Go field name.
// Nullability may have synthesized an identical tag on Field, so inspect the
// column source before synthesis rather than guessing from the final tag.
func authoredEmptyJSONName(source []*spec.View, field string) bool {
	if len(source) == 0 || source[0] == nil {
		return false
	}
	for _, column := range source[0].Columns {
		if column == nil || typecatalog.FieldName(column.Name) != field {
			continue
		}
		value, present := reflect.StructTag(column.Tag).Lookup("json")
		name, _, _ := strings.Cut(value, ",")
		return present && name == ""
	}
	return false
}

// bodyScalarJSONTag changes names only: inferred nullable omission was already
// selected by the view planner, and authored JSON options are preserved verbatim.
func bodyScalarJSONTag(plan *Plan, raw string) string {
	if _, present := reflect.StructTag(raw).Lookup("json"); present {
		return raw
	}
	if reflect.StructTag(raw).Get("internal") == "true" {
		return raw
	}
	if plan != nil && strings.TrimSpace(plan.Settings.Mutation) != "" && plan.Generation != nil && plan.Generation.WriterOmitEmpty {
		return appendStructTag(raw, "json", ",omitempty")
	}
	return raw
}

// Native writer presentation includes physical roots as well as auxiliary ones.
// This graph does not grant ordinary shared-row ownership or reader eligibility.
func nativeWriterPresentationGraph(root *spec.View) map[*spec.View]bool {
	result := map[*spec.View]bool{}
	var visit func(*spec.View)
	visit = func(view *spec.View) {
		if view == nil || result[view] || view.InMemory {
			return
		}
		result[view] = true
		for _, relation := range view.Relations {
			if relation != nil && relation.Kind != spec.RelationKindDerived {
				visit(relation.View)
			}
		}
	}
	visit(root)
	return result
}
