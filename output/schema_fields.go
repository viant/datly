package output

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/viant/mcp-protocol/schema/fieldname"
	xshape "github.com/viant/x/shape"
	"strings"

	"github.com/viant/tagly/format"
	"github.com/viant/tagly/format/text"
)

type jsonSchemaField struct {
	representation *xshape.Representation
	field          reflect.StructField
	name           string
	path           string
	required       bool
	quoted         bool
	nullable       bool
	timeLayout     string
}

func (p *Plan) jsonSchemaFields(t reflect.Type, path string, optional bool, active map[reflect.Type]bool, representations ...*xshape.Representation) ([]jsonSchemaField, error) {
	representation := representationForType(firstRepresentation(representations), t)
	if !p.transformedJSON() {
		return p.standardSchemaFields(t, path, representation)
	}
	if active[t] {
		return nil, fmt.Errorf("%w: recursive inline type %s", ErrSchemaUnavailable, t)
	}
	active[t] = true
	defer delete(active, t)
	var result []jsonSchemaField
	inlineCount, explicitSibling := 0, false
	for i := 0; i < t.NumField(); i++ {
		original := t.Field(i)
		field := original
		var fieldShape *xshape.Representation
		if representation != nil {
			field = representation.Field(i)
			fieldShape = representation.FieldShape(i)
		}
		if !field.IsExported() || field.Tag.Get("setMarker") == "true" {
			continue
		}
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
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
		fieldPath := schemaFieldPath(path, original.Name)
		inline := field.Anonymous || field.Tag.Get("jsonx") == "inline" || f.Inline
		if inline && !field.Anonymous {
			inlineCount++
		} else if !inline && explicit {
			explicitSibling = true
		}
		if p.exclude.Exclude(path, original.Name) {
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
			children, err := p.jsonSchemaFields(child, fieldPath, childOptional, active, representationForType(fieldShape, child))
			if err != nil {
				return nil, err
			}
			result = append(result, children...)
			continue
		}
		result = append(result, jsonSchemaField{representation: fieldShape, field: field, name: name, path: fieldPath,
			required: !optional && fieldname.Required(field), nullable: f.IsNullable(), timeLayout: f.TimeLayout})
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

// SelectorFieldNames uses the compiled output encoder's naming and visibility.
type SelectorFieldName struct {
	GoName     string
	PublicName string
	Index      []int
}

func (p *Plan) SelectorFieldNames(t reflect.Type, path string) ([]SelectorFieldName, error) {
	for t != nil && (t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array) {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct || p.custom != nil || selectorOpaqueJSON(t) {
		return nil, nil
	}
	var representation *xshape.Representation
	if p.representation != nil {
		var err error
		representation, err = p.representation.Lookup(path)
		if err != nil {
			return nil, err
		}
		for representation.Original().Kind() == reflect.Pointer || representation.Original().Kind() == reflect.Slice || representation.Original().Kind() == reflect.Array {
			representation = representation.Elem()
		}
		// Internal Current readers can share this component without belonging to
		// its public output graph. Representation indexes belong to one exact
		// storage type; do not apply them to an unrelated reader row.
		if representation.Original() != t {
			return nil, nil
		}
	}
	fields, err := p.jsonSchemaFields(t, path, false, map[reflect.Type]bool{}, representation)
	if err != nil {
		return nil, err
	}
	result := make([]SelectorFieldName, 0, len(fields))
	for _, field := range fields {
		result = append(result, SelectorFieldName{GoName: field.field.Name, PublicName: field.name, Index: append([]int(nil), field.field.Index...)})
	}
	return result, nil
}

func selectorOpaqueJSON(t reflect.Type) bool {
	iface := reflect.TypeFor[json.Marshaler]()
	return t.Implements(iface) || reflect.PointerTo(t).Implements(iface)
}

func firstRepresentation(representations []*xshape.Representation) *xshape.Representation {
	if len(representations) == 0 {
		return nil
	}
	return representations[0]
}
func representationForType(representation *xshape.Representation, t reflect.Type) *xshape.Representation {
	if representation == nil || !representation.Changed() {
		return nil
	}
	for representation.Original() != t && representation.Original().Kind() == reflect.Pointer {
		representation = representation.Elem()
	}
	return representation
}
