package report

import (
	"fmt"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/mcp-protocol/schema/fieldname"
	xshape "github.com/viant/x/shape"
	"reflect"
	"strings"
)

// sourceFilterPaths connects report filter fields only through their canonical
// source binding locations. Output visibility never erases trusted predicates.
func sourceFilterPaths(component *spec.Component, sourceType reflect.Type, filters []filter, targetType reflect.Type) (map[string][]int, error) {
	bindings, err := dtag.NewBindingIndex(sourceType)
	if err != nil {
		return nil, err
	}
	result := map[string][]int{}
	for _, filter := range filters {
		var matched *spec.Parameter
		for _, parameter := range component.Parameters {
			if parameter != nil && parameter.Source.Kind == filter.location.Kind && parameter.Source.Name == filter.location.In {
				if matched != nil {
					return nil, fmt.Errorf("ambiguous report filter source binding %s", filter.name)
				}
				matched = parameter
			}
		}
		if matched == nil {
			return nil, fmt.Errorf("report filter %s has no exact source binding", filter.name)
		}
		field, ok, e := bindings.Resolve(matched)
		if e != nil {
			return nil, e
		}
		if !ok {
			return nil, fmt.Errorf("report filter %s has no source input field", filter.name)
		}
		path, e := definitionPath(targetType, filter.index)
		if e != nil {
			return nil, e
		}
		result[path] = field.Index
	}
	return result, nil
}

func contractInputFieldVisible(owner reflect.Type, representation *xshape.Representation, index []int) (bool, error) {
	for _, position := range index {
		owner = dereference(owner)
		for representation != nil && representation.Original().Kind() == reflect.Pointer {
			representation = representation.Elem()
		}
		field := owner.Field(position)
		if representation != nil {
			field = representation.Field(position)
		}
		native, err := fieldname.Resolve(field, field.Name)
		if err != nil {
			return false, err
		}
		jsonName, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if native.Hidden || jsonName == "-" || field.Tag.Get("internal") == "true" || field.Tag.Get("setMarker") == "true" {
			return false, nil
		}
		owner = field.Type
		if representation != nil {
			representation = representation.FieldShape(position)
		}
	}
	return true, nil
}

// contractInputField reads exact original indexes through selected metadata;
// no spelling match links this input to an output or a different filter.
func contractInputField(owner reflect.Type, metadata *xshape.Representation, index []int) (reflect.StructField, error) {
	var field reflect.StructField
	for _, position := range index {
		owner = dereference(owner)
		for metadata != nil && metadata.Original().Kind() == reflect.Pointer {
			metadata = metadata.Elem()
		}
		field = owner.Field(position)
		if metadata != nil {
			field = metadata.Field(position)
		}
		owner = field.Type
		if metadata != nil {
			metadata = metadata.FieldShape(position)
		}
	}
	return field, nil
}
