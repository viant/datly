package converter

import (
	"fmt"
	"reflect"
	"strconv"
	"time"
)

var TimeType = reflect.TypeOf(time.Time{})

func Convert(raw string, toType reflect.Type, format string) (interface{}, error) {
	switch toType.Kind() {
	case reflect.Bool:
		return strconv.ParseBool(raw)
	case reflect.Int:
		if raw == "" {
			return 0, nil
		}

		return strconv.Atoi(raw)
	case reflect.Int8:
		if raw == "" {
			return int8(0), nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, err
		}
		return int8(asInt), nil

	case reflect.Int16:
		if raw == "" {
			return int16(0), nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, err
		}
		return int16(asInt), nil

	case reflect.Int32:
		if raw == "" {
			return int32(0), nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, err
		}
		return int32(asInt), nil

	case reflect.Int64:
		if raw == "" {
			return int64(0), nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, err
		}
		return int64(asInt), nil

	case reflect.Uint:
		if raw == "" {
			return uint(0), nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, err
		}

		return uint(asInt), err
	case reflect.Uint8:
		if raw == "" {
			return uint8(0), nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, err
		}
		return uint8(asInt), nil

	case reflect.Uint16:
		if raw == "" {
			return uint16(0), nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, err
		}
		return uint16(asInt), nil

	case reflect.Uint32:
		if raw == "" {
			return uint32(0), nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, err
		}
		return uint32(asInt), nil

	case reflect.Uint64:
		if raw == "" {
			return uint64(0), nil
		}

		asInt, err := strconv.Atoi(raw)
		if err != nil {
			return nil, err
		}
		return uint64(asInt), nil

	case reflect.Float64:
		if raw == "" {
			return 0.0, nil
		}

		return strconv.ParseFloat(raw, 64)

	case reflect.Float32:
		if raw == "" {
			return float32(0.0), nil
		}

		asFloat, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, err
		}

		return float32(asFloat), nil

	case reflect.String:
		return raw, nil
	case reflect.Struct:
		if toType == TimeType {
			if format == "" {
				format = time.RFC3339
			}

			return time.Parse(format, raw)
		}
	}

	return nil, fmt.Errorf("unsupported convert dest type %v", toType)
}
