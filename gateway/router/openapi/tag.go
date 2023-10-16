package openapi

import (
	"github.com/viant/govalidator"
	"github.com/viant/structology/format"
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
		Example      interface{}
	}
)

func ParseTag(field reflect.StructField, tag reflect.StructTag) (Tag, error) {
	aTag, err := format.Parse(tag, "json")
	if err != nil {
		return Tag{}, err
	}

	validationTag := govalidator.ParseTag(string(tag))
	if err != nil {
		return Tag{}, err
	}

	return Tag{
		Format:      aTag.DateFormat,
		Inlined:     aTag.Inline,
		Ignore:      aTag.Ignore,
		IsNullable:  !validationTag.Required && field.Type.Kind() == reflect.Ptr,
		CaseFormat:  aTag.CaseFormat,
		Description: tag.Get("description"),
	}, nil
}
