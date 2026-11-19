package openapi

import (
	"github.com/viant/datly/gateway/router/openapi/openapi3"
	"github.com/viant/tagly/choice"
)

func fieldEnum(result *openapi3.Schema, field *Schema) (*openapi3.Schema, error) {
	enumeration, err := choice.ParseEnum(field.rawTag, field.rType)
	if err != nil {
		return nil, err
	}
	if enumeration != nil {
		result.Type = enumeration.WireType(field.isInput)
		result.Format = ""
		result.Enum = enumeration.WireValues(field.isInput)
		return result, nil
	}
	constraint, err := choice.Parse(field.rawTag, field.rType)
	if err != nil {
		return nil, err
	}
	if constraint != nil {
		result.Enum = constraint.WireValues()
	}
	return result, nil
}
