package output

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/tagly/format"
	"github.com/viant/tagly/format/text"
)

type jsonSchemaField struct {
	field      reflect.StructField
	name       string
	path       string
	required   bool
	quoted     bool
	nullable   bool
	timeLayout string
}

func (p *Plan) jsonSchemaFields(t reflect.Type, path string, optional bool, active map[reflect.Type]bool) ([]jsonSchemaField, error) {
	if !p.transformedJSON() {
		return p.standardSchemaFields(t, path)
	}
	if active[t] {
		return nil, fmt.Errorf("%w: recursive inline type %s", ErrSchemaUnavailable, t)
	}
	active[t] = true
	defer delete(active, t)
	var result []jsonSchemaField
	inlineCount, explicitSibling := 0, false
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() || field.Tag.Get("setMarker") == "true" {
			continue
		}
		name, options, _ := strings.Cut(field.Tag.Get("json"), ",")
		explicit := name != ""
		if name == "-" || field.Tag.Get("internal") == "true" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		// Use the same format-tag parser and name precedence as native JSON.
		f, err := format.Parse(field.Tag)
		if err != nil || f == nil {
			f = &format.Tag{}
		}
		if f.Ignore {
			continue
		}
		caseExplicit := explicit
		if !explicit && (f.Name != "" || f.CaseFormat != "") {
			if f.Name == "" {
				f.Name = name
			}
			name = f.CaseFormatName("")
			explicit, caseExplicit = true, f.CaseFormat != ""
		}
		if !caseExplicit {
			name = p.schemaCaseName(name)
		}
		fieldPath := schemaFieldPath(path, field.Name)
		inline := field.Anonymous || field.Tag.Get("jsonx") == "inline" || f.Inline
		if inline && !field.Anonymous {
			inlineCount++
		} else if !inline && explicit {
			explicitSibling = true
		}
		if p.exclude.Exclude(path, field.Name) {
			continue
		}
		if inline {
			child := field.Type
			childOptional := optional
			for child.Kind() == reflect.Pointer {
				child, childOptional = child.Elem(), true
			}
			if child.Kind() != reflect.Struct {
				return nil, fmt.Errorf("%w: opaque inline field %s", ErrSchemaUnavailable, field.Name)
			}
			children, err := p.jsonSchemaFields(child, fieldPath, childOptional, active)
			if err != nil {
				return nil, err
			}
			result = append(result, children...)
			continue
		}
		omit := f.Omitempty || p.omitEmpty || optional
		for _, option := range strings.Split(options, ",") {
			omit = omit || option == "omitempty"
		}
		result = append(result, jsonSchemaField{field: field, name: name, path: fieldPath,
			required: !omit, nullable: f.IsNullable(), timeLayout: f.TimeLayout})
	}
	if inlineCount == 1 && !explicitSibling {
		return nil, fmt.Errorf("%w: whole-value inline encoding on %s", ErrSchemaUnavailable, t)
	}
	return result, nil
}

func (p *Plan) schemaCaseName(name string) string {
	if p.caseFormat == "" {
		return name
	}
	if name == "ID" && (p.caseFormat == text.CaseFormatLower || p.caseFormat == text.CaseFormatLowerCamel || p.caseFormat == text.CaseFormatLowerUnderscore) {
		return "id"
	}
	source := text.DetectCaseFormat(name)
	if !source.IsDefined() {
		source = text.CaseFormatUpperCamel
	}
	return source.Format(name, p.caseFormat)
}

func schemaFieldPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "." + name
}
