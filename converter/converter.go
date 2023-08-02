package converter

import (
	"context"
	"encoding/json"
	"github.com/viant/toolbox"
	"github.com/viant/xreflect"
	"reflect"
	"strconv"
	"time"
)

type Unmarshaller func([]byte, interface{}) error

func Convert(raw string, toType reflect.Type, skipValidation bool, format string, options ...interface{}) (value interface{}, wasNil bool, err error) {
	switch toType.Kind() {
	case reflect.Slice:
		switch toType.Elem().Kind() {
		case reflect.Int:
			v, err := NewRepeated(raw, true).AsInts()
			return v, false, err
		case reflect.Uint64:
			v, err := NewRepeated(raw, true).AsUInt64s()
			return v, false, err
		case reflect.Int64:
			v, err := NewRepeated(raw, true).AsInt64s()
			return v, false, err
		case reflect.Uint:
			v, err := NewRepeated(raw, true).AsUInts()
			return v, false, err
		case reflect.String:
			v := NewRepeated(raw, false)
			return []string(v), false, nil
		case reflect.Float64:
			v, err := NewRepeated(raw, true).AsFloats64()
			return v, false, err
		case reflect.Float32:
			v, err := NewRepeated(raw, true).AsFloats32()
			return v, false, err

		}
	case reflect.Bool:
		parseBool, err := strconv.ParseBool(raw)
		return parseBool, false, err
	case reflect.Int:
		if raw == "" {
			return 0, false, nil
		}
		atoi, err := strconv.Atoi(raw)
		return atoi, false, err
	case reflect.Int8:
		if raw == "" {
			return int8(0), false, nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, false, err
		}
		return int8(asInt), false, nil

	case reflect.Int16:
		if raw == "" {
			return int16(0), false, nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, false, err
		}
		return int16(asInt), false, nil

	case reflect.Int32:
		if raw == "" {
			return int32(0), false, nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, false, err
		}
		return int32(asInt), false, nil

	case reflect.Int64:
		if raw == "" {
			return int64(0), false, nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, false, err
		}
		return int64(asInt), false, nil

	case reflect.Uint:
		if raw == "" {
			return uint(0), false, nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, false, err
		}

		return uint(asInt), false, err
	case reflect.Uint8:
		if raw == "" {
			return uint8(0), false, nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, false, err
		}
		return uint8(asInt), false, nil

	case reflect.Uint16:
		if raw == "" {
			return uint16(0), false, nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, false, err
		}
		return uint16(asInt), false, nil

	case reflect.Uint32:
		if raw == "" {
			return uint32(0), false, nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, false, err
		}
		return uint32(asInt), false, nil

	case reflect.Uint64:
		if raw == "" {
			return uint64(0), false, nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, false, err
		}
		return uint64(asInt), false, nil

	case reflect.Float64:
		if raw == "" {
			return 0.0, false, nil
		}

		asFloat, err := strconv.ParseFloat(raw, 64)
		return asFloat, false, err

	case reflect.Float32:
		if raw == "" {
			return float32(0.0), false, nil
		}

		asFloat, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, false, err
		}

		return float32(asFloat), false, nil

	case reflect.String:
		return raw, false, nil
	case reflect.Struct:
		if toType == xreflect.TimeType {
			if format == "" {
				format = time.RFC3339
			}

			asTime, err := toolbox.ToTime(raw, format)
			if err != nil {
				return nil, false, err
			}

			return *asTime, false, nil
		}
	}

	var wasPtr bool
	if toType.Kind() == reflect.Ptr {
		toType = toType.Elem()
		wasPtr = true
	}

	dest := reflect.New(toType)

	if raw != "" {
		unmarshall := unmarshaller(options)
		err = unmarshall([]byte(raw), dest.Interface())
		if err != nil {
			return nil, false, err
		}
	}

	isNil := dest.IsNil()
	if !wasPtr {
		dest = dest.Elem()
	}

	result := dest.Interface()
	if isNil {
		return result, isNil, nil
	}

	if toType.Kind() == reflect.Struct && !skipValidation {
		validation, err := aValidator.Validate(context.Background(), result)
		if err != nil {
			return nil, false, err
		}
		if validation.Failed {
			return nil, false, validation
		}
	}

	if err != nil {
		return nil, false, err
	}

	return result, isNil, nil
}

func unmarshaller(options []interface{}) Unmarshaller {
	for _, option := range options {
		switch actual := option.(type) {
		case Unmarshaller:
			return actual
		}
	}

	return json.Unmarshal
}
