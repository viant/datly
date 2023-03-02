package json

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

const (
	DefaultTagName    = "default"
	FormatAttribute   = "format"
	ValueAttribute    = "value"
	NullableAttribute = "nullable"
	RequiredAttribute = "required"
	Embedded          = "embedded"
)

type DefaultTag struct {
	Format   string
	Value    string
	Nullable *bool
	Required *bool
	Embedded bool

	_value interface{}
	_ptr   unsafe.Pointer
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
		case NullableAttribute:
			t.Nullable = booleanPtr(keyValue[1] == "true")
		case RequiredAttribute:
			t.Required = booleanPtr(keyValue[1] == "true")
		case Embedded:
			t.Embedded = keyValue[1] == "true"
		}
	}

	if t.Value != "" {
		var err error
		t._value, t._ptr, err = parseValue(field.Type, t.Value, t.Format)
		if err != nil {
			return err
		}
	}

	return nil
}

func booleanPtr(b bool) *bool {
	return &b
}

func parseValue(rType reflect.Type, rawValue string, timeFormat string) (interface{}, unsafe.Pointer, error) {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	switch rType.Kind() {
	case reflect.String:
		return rawValue, unsafe.Pointer(&rawValue), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		asInt, err := strconv.Atoi(rawValue)
		if err != nil {
			return nil, nil, err
		}

		return asInt, unsafe.Pointer(&asInt), nil

	case reflect.Bool:
		asBool, err := strconv.ParseBool(rawValue)
		if err != nil {
			return nil, nil, err
		}
		return asBool, unsafe.Pointer(&asBool), nil

	case reflect.Float64, reflect.Float32:
		asFloat, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			return nil, nil, err
		}
		return asFloat, unsafe.Pointer(&asFloat), nil

	case reflect.Struct:
		if timeType == rType {
			asTime, err := time.Parse(timeFormat, rawValue)
			if err != nil {
				return nil, nil, err
			}

			return &asTime, unsafe.Pointer(&asTime), nil
		}
	}

	return nil, nil, fmt.Errorf("unsupported type %v", rType.String())
}

func (t *DefaultTag) IsRequired() bool {
	return t.Required != nil && *t.Required
}

func (t *DefaultTag) IsNullable() bool {
	return t.Nullable != nil && *t.Nullable
}
