package openapi

import (
	"github.com/viant/datly/shared"
	"github.com/viant/govalidator"
	"github.com/viant/structology/format"
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

		_tag     format.Tag
		TypeName string
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

	typeName := tag.Get(xreflect.TagTypeName)
	if (field.Type.Kind() == reflect.Slice || field.Type.Kind() == reflect.Array) && typeName == "" {
		typeName = field.Type.Name()
	}

	rType := shared.Elem(field.Type)
	if typeName == "" && rType.Name() != "" && rType.PkgPath() != "time" && rType.Kind() == reflect.Struct {
		typeName = rType.String()
	}

	return Tag{
		Format:      aTag.DateFormat,
		Inlined:     aTag.Inline,
		Ignore:      aTag.Ignore,
		IsNullable:  !validationTag.Required && field.Type.Kind() == reflect.Ptr,
		TypeName:    typeName,
		CaseFormat:  aTag.CaseFormat,
		Description: tag.Get("description"),
		Example:     tag.Get("example"),
		_tag:        *aTag,
	}, nil
}
