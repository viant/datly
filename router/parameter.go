package router

import (
	"fmt"
	"github.com/viant/datly/view"
	"reflect"
	"strconv"
	"strings"
)

func BuildParameter(field reflect.StructField) (*view.Parameter, error) {
	result := &view.Parameter{}
	paramTag := field.Tag.Get("parameter")

	split := strings.Split(paramTag, ",")
	for _, paramPart := range split {
		keyValue := strings.Split(paramPart, "=")
		if len(keyValue) != 2 {
			return nil, fmt.Errorf("unsupported Key=Value format: %v", paramPart)
		}
		key := keyValue[0]
		value := keyValue[1]

		switch strings.ToLower(key) {
		case "ref":
			result.Ref = value
		case "name":
			result.Name = value

		case "target":
			if result.In == nil {
				result.In = &view.Location{}
			}

			result.In.Name = value
		case "kind":
			if result.In == nil {
				result.In = &view.Location{}
			}

			result.In.Kind = view.Kind(value)

		case "required":
			parseBool, err := strconv.ParseBool(value)
			if err != nil {
				return nil, err
			}

			result.Required = &parseBool

		case "datatype":
			if result.Schema == nil {
				result.Schema = &view.Schema{}
			}

			result.Schema.DataType = value

		case "cardinality":
			if result.Schema == nil {
				result.Schema = &view.Schema{}
			}

			result.Schema.Cardinality = view.Cardinality(value)
		}
	}

	if result.Name == "" {
		result.Name = field.Name
	}

	return result, nil
}
