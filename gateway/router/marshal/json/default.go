package json

import (
	"encoding/json"
	"fmt"
	"github.com/viant/xreflect"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultTagName = "default"

	FormatAttribute     = "format"
	IgnoreCaseFormatter = "ignorecaseformatter"

	ValueAttribute    = "value"
	NullableAttribute = "nullable"
	NameAttribute     = "name"
	RequiredAttribute = "required"
	Embedded          = "embedded"
)

type DefaultTag struct {
	Format              string
	Name                string
	IgnoreCaseFormatter bool
	Value               string
	Nullable            *bool
	Required            *bool
	Embedded            bool

	_value interface{}
}

func NewDefaultTag(field reflect.StructField) (*DefaultTag, error) {
	aTag := &DefaultTag{}
	if err := aTag.Init(field); err != nil {
		return nil, err
	}
	return aTag, nil
}

func (t *DefaultTag) Init(field reflect.StructField) error {
	tagValue := field.Tag.Get(DefaultTagName)
	if tagValue == "" {
		return nil
	}

	attributes := strings.Split(tagValue, ",")
	for _, attribute := range attributes {
		keyValue := strings.Split(attribute, "=")
		if len(keyValue) != 2 {
			return fmt.Errorf("unsupported attribute %v format", attribute)
		}

		switch strings.ToLower(keyValue[0]) {
		case ValueAttribute:
			t.Value = keyValue[1]
		case FormatAttribute:
			t.Format = keyValue[1]
		case IgnoreCaseFormatter:
			t.IgnoreCaseFormatter = keyValue[1] == "true"
		case NullableAttribute:
			t.Nullable = booleanPtr(keyValue[1] == "true")
		case RequiredAttribute:
			t.Required = booleanPtr(keyValue[1] == "true")
		case NameAttribute:
			t.Name = keyValue[1]
		case Embedded:
			t.Embedded = keyValue[1] == "true"
		}
	}

	if t.Value != "" {
		var err error
		t._value, err = parseValue(field.Type, t.Value, t.Format)
		if err != nil {
			return err
		}
	}

	return nil
}

func booleanPtr(b bool) *bool {
	return &b
}

func parseValue(rType reflect.Type, rawValue string, timeFormat string) (interface{}, error) {
	elemType := rType
	var dereferenced bool
	for elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
		dereferenced = true
	}

	if elemType == xreflect.TimeType {
		if timeFormat == "" {
			timeFormat = time.RFC3339
		}

		parse, err := time.Parse(timeFormat, rawValue)
		if err != nil {
			return nil, err
		}

		if dereferenced {
			return &parse, err
		}

		return parse, err
	}

	if elemType.Kind() == reflect.String {
		rawValue = strconv.Quote(rawValue)
	}

	if dereferenced {
		elemType = reflect.PtrTo(elemType)
	}

	rValue := reflect.New(elemType)
	if err := json.Unmarshal([]byte(rawValue), rValue.Interface()); err != nil {
		return nil, err
	}

	return rValue.Elem().Interface(), nil
}

func (t *DefaultTag) IsRequired() bool {
	return t.Required != nil && *t.Required
}

func (t *DefaultTag) IsNullable() bool {
	return t.Nullable != nil && *t.Nullable
}
