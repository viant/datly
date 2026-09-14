package fragment

import (
	"reflect"

	xpredicate "github.com/viant/xdatly/predicate"
)

func predicateValues(input any, include bool) []any {
	if input == nil {
		return nil
	}
	switch actual := input.(type) {
	case *xpredicate.StringsFilter:
		if actual == nil {
			return nil
		}
		return stringValues(selectFilterValues(actual.Include, actual.Exclude, include))
	case xpredicate.StringsFilter:
		return stringValues(selectFilterValues(actual.Include, actual.Exclude, include))
	case *xpredicate.IntFilter:
		if actual == nil {
			return nil
		}
		return intValues(selectFilterValues(actual.Include, actual.Exclude, include))
	case xpredicate.IntFilter:
		return intValues(selectFilterValues(actual.Include, actual.Exclude, include))
	case *xpredicate.BoolFilter:
		if actual == nil {
			return nil
		}
		return boolValues(selectFilterValues(actual.Include, actual.Exclude, include))
	case xpredicate.BoolFilter:
		return boolValues(selectFilterValues(actual.Include, actual.Exclude, include))
	}
	return flattenPredicateValues(reflect.ValueOf(input))
}

func flattenPredicateValues(value reflect.Value) []any {
	for value.IsValid() && (value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer) {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return nil
	}
	switch value.Kind() {
	case reflect.Slice, reflect.Array:
		result := make([]any, 0, value.Len())
		for i := 0; i < value.Len(); i++ {
			result = append(result, flattenPredicateValues(value.Index(i))...)
		}
		return result
	default:
		return []any{value.Interface()}
	}
}

func stringValues(input []string) []any {
	result := make([]any, 0, len(input))
	for _, item := range input {
		result = append(result, item)
	}
	return result
}

func intValues(input []int) []any {
	result := make([]any, 0, len(input))
	for _, item := range input {
		result = append(result, item)
	}
	return result
}

func boolValues(input []bool) []any {
	result := make([]any, 0, len(input))
	for _, item := range input {
		result = append(result, item)
	}
	return result
}

func selectFilterValues[T any](includeVals, excludeVals []T, include bool) []T {
	if include {
		if len(includeVals) > 0 {
			return append([]T(nil), includeVals...)
		}
		return nil
	}
	if len(excludeVals) > 0 {
		return append([]T(nil), excludeVals...)
	}
	return nil
}
