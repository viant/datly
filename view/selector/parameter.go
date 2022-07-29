package selector

import (
	"fmt"
	"reflect"
)

var (
	stringType = reflect.TypeOf("")
	intType    = reflect.TypeOf(0)
)

const (
	Fields   = "_fields"
	Offset   = "_offset"
	OrderBy  = "_orderby"
	Limit    = "_limit"
	Criteria = "_criteria"
	Page     = "_page"
)

func ParamType(name string) reflect.Type {
	switch name {
	case Limit, Offset, Page:
		return intType
	default:
		return stringType
	}
}

func Description(paramName, viewName string) string {
	switch paramName {
	case Limit:
		return fmt.Sprintf("allows to limit %v view data returned from db", viewName)
	case Offset:
		return fmt.Sprintf("allows to skip first n  view %v records, it has to be used alongside the limit", viewName)
	case Criteria:
		return fmt.Sprintf("allows to filter view %v data that matches given criteria", viewName)
	case Fields:
		return fmt.Sprintf("allows to control view %v fields present in response", viewName)
	case OrderBy:
		return fmt.Sprintf("allows to sort view %v results", viewName)
	case Page:
		return fmt.Sprintf("allows to skip first page * limit values, starting from 0 page. Has precedence over offset")
	}
	return ""
}
