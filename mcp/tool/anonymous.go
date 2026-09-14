package tool

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/runtime/registry"
	xshape "github.com/viant/x/shape"
)

func (c *Compiler) compileAnonymousBody(inputField registry.InputField) ([]Argument, error) {
	binding := inputField.Binding()
	if !strings.EqualFold(strings.TrimSpace(binding.Location.Kind), "body") {
		return nil, fmt.Errorf("anonymous field %q must use body binding, got %q", inputField.Path(), binding.Location.Kind)
	}
	if strings.TrimSpace(binding.Location.In) != "" {
		return nil, fmt.Errorf("anonymous body field %q cannot use named body source %q", inputField.Path(), binding.Location.In)
	}
	types := xshape.Runtime{}
	sourceType := types.Indirect(inputField.SourceType())
	destinationType := types.Indirect(inputField.DestinationType())
	if sourceType == nil || sourceType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("anonymous body field %q source type must be a struct, got %v", inputField.Path(), inputField.SourceType())
	}
	if sourceType != destinationType {
		return nil, fmt.Errorf("anonymous body field %q requires matching source and destination structs, got %v and %v", inputField.Path(), inputField.SourceType(), inputField.DestinationType())
	}
	fields, err := xshape.Linked(sourceType).JSONFields()
	if err != nil {
		return nil, err
	}
	result := make([]Argument, 0, len(fields))
	for _, projected := range fields {
		if !projected.Field.Exported {
			continue
		}
		field := projected.Field.StructField()
		publicName := projected.Name
		hidden := field.Tag.Get("setMarker") == "true"
		if hidden {
			continue
		}
		result = append(result, Argument{
			documentation: inputField.Documentation(), publicName: publicName, path: inputField.Path() + "." + field.Name,
			sourceKind: "body", sourceName: publicName,
			sourceType: field.Type, destinationType: field.Type,
		})
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("anonymous body field %q has no exported JSON fields", inputField.Path())
	}
	return result, nil
}
