package types

import (
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

func GetOrParseType(typeLookup xreflect.TypeLookupFn, dataType string) (reflect.Type, error) {
	lookup, lookupErr := typeLookup("", "", dataType)
	if lookupErr == nil {
		return lookup, nil
	}

	parseType, parseErr := ParseType(dataType, typeLookup)
	if parseErr == nil {
		return parseType, nil
	}

	return nil, fmt.Errorf("couldn't determine struct type: %v, due to the: %w, %v", dataType, lookupErr, parseErr)
}

func ParseType(dataType string, typeLookup xreflect.TypeLookupFn) (reflect.Type, error) {
	precisionIndex := strings.Index(dataType, "(")
	if precisionIndex != -1 {
		dataType = dataType[:precisionIndex]
	}

	rType, ok := io.ParseType(dataType)
	if ok {
		return rType, nil
	}

	return xreflect.ParseWithLookup(dataType, true, typeLookup)
}

func Elem(rType reflect.Type) reflect.Type {
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}

	return rType
}
