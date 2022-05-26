package converter

import (
	"fmt"
	"reflect"
	"strconv"
)

func Convert(raw string, toType reflect.Type) (interface{}, error) {
	switch toType.Kind() {
	case reflect.Bool:
		return strconv.ParseBool(raw)
	case reflect.Int:
		return strconv.Atoi(raw)
	case reflect.Float64:
		return strconv.ParseFloat(raw, 64)
	case reflect.String:
		return raw, nil
	}

	return nil, fmt.Errorf("unsupported convert dest type %v", toType)
}

// time.RFC3339
