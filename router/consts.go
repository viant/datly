package router

import (
	"fmt"
	"reflect"
)

type QueryParam string

var (
	stringType = reflect.TypeOf("")
	intType    = reflect.TypeOf(0)
)

const (
	Fields   QueryParam = "_fields"
	Offset   QueryParam = "_offset"
	OrderBy  QueryParam = "_orderby"
	Limit    QueryParam = "_limit"
	Criteria QueryParam = "_criteria"
	Page     QueryParam = "_page"
)

func (q QueryParam) ParamType() reflect.Type {
	switch q {
	case Limit, Offset, Page:
		return intType
	default:
		return stringType
	}
}

func (q QueryParam) Description(viewName string) string {
	switch q {
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
		return fmt.Sprintf("allows to skip first page * limit values, starting from 1 page. Has precedence over offset")
	}

	return ""
}
