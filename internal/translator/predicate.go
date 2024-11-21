package translator

import (
	"fmt"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/view/state"
	"github.com/viant/tagly/format/text"
	"reflect"
	"strings"
)

// generateInputPredicate to create the flattened struct
func (s *Service) generateInputPredicate(t reflect.Type, alias string, resource *Resource) {
	rootPkgPath := t.PkgPath()
	addPredicateParameters(t, "", rootPkgPath, alias, resource)

}

// Helper function to recursively flatten struct fields
func addPredicateParameters(t reflect.Type, prefix string, rootPkgPath string, alias string, resource *Resource) {
	switch t.Kind() {
	case reflect.Ptr:
		addPredicateParameters(t.Elem(), prefix, rootPkgPath, alias, resource)
	case reflect.Struct:
		if t.PkgPath() == rootPkgPath || t.PkgPath() == "" {
			// Inline the struct
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				fieldName := field.Name

				sqlxTag := field.Tag.Get("json")
				if sqlxTag == "-" {
					continue
				}
				jsonTag := field.Tag.Get("json")
				if jsonTag == "-" {
					continue
				}
				jsonFieldName := strings.Split(jsonTag, ",")[0]
				if jsonFieldName != "" {
					fieldName = jsonFieldName
				}
				newPrefix := prefix
				if newPrefix != "" {
					newPrefix += "."
				}
				newPrefix += fieldName
				addPredicateParameters(field.Type, newPrefix, rootPkgPath, alias, resource)
			}
		} else {
			// For types outside the root package, treat as leaf
			addPredicateParameter(t, prefix, resource)
		}
	case reflect.Slice, reflect.Array:
		// For slices or arrays, treat as leaf
		addPredicateParameters(t.Elem(), prefix, rootPkgPath, alias, resource)
	default:
		// For basic types, add field
		addPredicateParameter(t, prefix, resource)
	}
}

// Helper function to add a field to the fields slice
func addPredicateParameter(t reflect.Type, prefix string, resource *Resource) {
	// Determine field name

	fieldName := strings.ToLower(strings.ReplaceAll(prefix, ".", "_"))
	fieldName = text.CaseFormatLowerUnderscore.Format(fieldName, text.CaseFormatUpperCamel)

	// Determine field type and predicate
	var fieldType reflect.Type
	var predicate string

	// Check if the last part of the prefix ends with "id" (case-insensitive)
	parts := strings.Split(prefix, ".")
	lastPart := parts[len(parts)-1]
	if strings.HasSuffix(strings.ToLower(lastPart), "id") {
		// Use "in" predicate and slice type
		fieldType = reflect.SliceOf(reflect.TypeOf(""))
		predicate = fmt.Sprintf("in,group=0,a,%s", prefix)
	} else if t.Kind() == reflect.String || (t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.String) {
		// Use "like" predicate and string type
		fieldType = reflect.TypeOf("")
		predicate = fmt.Sprintf("like,group=0,a,%s", prefix)
	} else {
		// For other types, use their default type
		fieldType = t
		predicate = fmt.Sprintf("eq,group=0,a,%s", prefix)
	}

	if fieldName == "" {
		fmt.Println()
		return
	}
	parameter := inference.Parameter{}
	parameter.In = state.NewQueryLocation(prefix)
	parameter.Name = fieldName
	parameter.Schema = state.NewSchema(fieldType)
	parameter.Tag = fmt.Sprintf(`predicate:"%s"`, predicate)
	resource.State.Append(&parameter)
}
