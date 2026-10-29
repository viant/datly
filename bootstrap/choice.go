package bootstrap

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
	xshape "github.com/viant/x/shape"
	"reflect"
	"strings"
)

func (r *packageComponentResolver) resolveChoiceMetadata(field xshape.Field, param *spec.Parameter) error {
	if param == nil {
		return nil
	}
	raw := field.Tag.Get("wireSchema")
	var tagSchema *spec.WireSchema
	if raw != "" {
		tagSchema = &spec.WireSchema{}
		if err := json.Unmarshal([]byte(raw), tagSchema); err != nil {
			return err
		}
	}
	if !strings.Contains(string(field.Tag), "choice:") && (param.WireSchema == nil || len(param.WireSchema.Enum) == 0) && (tagSchema == nil || len(tagSchema.Enum) == 0) {
		return nil
	}
	typ := field.ReflectedType
	expression := param.WireTypeExpr
	if expression == "" {
		expression = param.TypeExpr
	}
	if expression != "" {
		resolved, err := (xshape.Runtime{Lookup: func(name string) (reflect.Type, error) {
			if r.types != nil {
				if declared, e := r.types.Type(name); e == nil && declared != nil {
					return declared, nil
				}
			}
			leaf := field.ReflectedType
			for leaf.Kind() == reflect.Pointer {
				leaf = leaf.Elem()
			}
			if leaf.PkgPath() != "" && (name == leaf.PkgPath()+"."+leaf.Name() || name == leaf.String()) {
				return leaf, nil
			}
			return nil, fmt.Errorf("choice source type %q unresolved", name)
		}}).Type(expression)
		if err != nil {
			return err
		}
		typ = resolved
	}
	normalized, err := choice.Parse(reflect.StructTag(field.Tag), typ)
	if err != nil {
		return fmt.Errorf("input %s choice: %w", field.Name, err)
	}
	for _, schema := range []*spec.WireSchema{param.WireSchema, tagSchema} {
		if schema == nil || len(schema.Enum) == 0 {
			continue
		}
		candidate, err := choice.Normalize(typ, schema.Enum)
		if err != nil {
			return fmt.Errorf("input %s choice compatibility: %w", field.Name, err)
		}
		expected := "number"
		switch candidate.Type.Kind() {
		case reflect.String:
			expected = "string"
		case reflect.Bool:
			expected = "boolean"
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			expected = "integer"
		}
		if schema.Type != "" && schema.Type != expected {
			return fmt.Errorf("input %s choice wire type conflict", field.Name)
		}
		if normalized != nil && !choice.Equal(normalized, candidate) {
			return fmt.Errorf("input %s conflicting choice authorities (including order)", field.Name)
		}
		normalized = candidate
	}
	if normalized == nil {
		return nil
	}
	schema := param.WireSchema.Clone()
	if schema == nil {
		schema = &spec.WireSchema{}
	}
	schema.Type = "number"
	switch normalized.Type.Kind() {
	case reflect.String:
		schema.Type = "string"
	case reflect.Bool:
		schema.Type = "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		schema.Type = "integer"
	}
	schema.Enum = normalized.WireValues()
	if typ.Kind() == reflect.Pointer {
		schema.Nullable = true
	}
	param.WireSchema = schema
	return nil
}
