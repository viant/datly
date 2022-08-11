package converter

import (
	"encoding/json"
	"reflect"
	"strconv"
	"time"
)

var TimeType = reflect.TypeOf(time.Time{})

func Convert(raw string, toType reflect.Type, format string) (value interface{}, wasNil bool, err error) {
	switch toType.Kind() {
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
		if toType == TimeType {
			if format == "" {
				format = time.RFC3339
			}

			asTime, err := time.Parse(format, raw)
			return asTime, false, err
		}
	}

	var wasPtr bool
	if toType.Kind() == reflect.Ptr {
		toType = toType.Elem()
		wasPtr = true
	}

	dest := reflect.New(toType)

	if raw != "" {
		err = json.Unmarshal([]byte(raw), dest.Interface())
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

	if toType.Kind() == reflect.Struct {
		err = aValidator.Struct(result)
	}

	if err != nil {
		return nil, false, err
	}

	return result, isNil, nil
}
