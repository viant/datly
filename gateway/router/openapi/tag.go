package openapi

import (
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/govalidator"
	"github.com/viant/tagly/format"
	"github.com/viant/xreflect"
	"reflect"
)

type (
	Tag struct {
		Format      string
		Inlined     bool
		Ignore      bool
		Description string
		CaseFormat  string

		IsNullable   bool
		Min          *float64
		Max          *float64
		ExclusiveMax bool
		ExclusiveMin bool
		MaxLength    *uint64
		MinLength    uint64
		WriteOnly    bool
		ReadOnly     bool
		MaxItems     *uint64
		Default      interface{}
		Example      string
		JSONName     string
		_tag         format.Tag
		TypeName     string
		Parameter    *tags.Parameter
	}
)

func ParseTag(field reflect.StructField, tag reflect.StructTag, isInput bool) (*Tag, error) {

	aTag, err := format.Parse(tag, "json", "openapi")
	if err != nil {
		return &Tag{}, err
	}

	validationTag := govalidator.ParseTag(string(tag))
	if err != nil {
		return &Tag{}, err
	}

	typeName := tag.Get(xreflect.TagTypeName)
	if (field.Type.Kind() == reflect.Slice || field.Type.Kind() == reflect.Array) && typeName == "" {
		typeName = field.Type.Name()
	}

	rType := shared.Elem(field.Type)
	if typeName == "" && rType.Name() != "" && rType.PkgPath() != "time" && rType.Kind() == reflect.Struct {
		typeName = rType.String()
	}
	jsonName := aTag.Name
	if aTag.Name != "" {
		jsonName = aTag.Name
	}

	ret := &Tag{
		Format:     aTag.DateFormat,
		Inlined:    aTag.Inline,
		Ignore:     aTag.Ignore,
		IsNullable: !validationTag.Required && field.Type.Kind() == reflect.Ptr,
		TypeName:   typeName,

		CaseFormat:  aTag.CaseFormat,
		Description: tag.Get(tags.DescriptionTag),
		Example:     tag.Get(tags.ExampleTag),
		JSONName:    jsonName,
		_tag:        *aTag,
	}

	if tags, _ := tags.Parse(tag, nil, tags.ParameterTag); tags != nil && isInput {
		ret.Parameter = tags.Parameter
		if parameter := ret.Parameter; parameter != nil && parameter.Kind != "" {
			switch state.Kind(parameter.Kind) {
			case state.KindForm, state.KindRequestBody:
			default:
				ret.Ignore = true
			}
		}
	}
	return ret, nil
}
