package keys

import (
	"github.com/viant/xdatly/handler/response"
	"github.com/viant/xdatly/predicate"
	"github.com/viant/xreflect"
	"reflect"
)

var Types = map[string]reflect.Type{

	//component/View related keys
	Error:   xreflect.StringType,
	Status:  reflect.TypeOf(response.Status{}),
	Metrics: reflect.TypeOf(response.Metrics{}),

	SQL:     xreflect.StringType,
	Filters: reflect.TypeOf(predicate.NamedFilters{}),

	//Response keys
	ResponseTime:          xreflect.TimeType,
	ResponseElapsedInSec:  xreflect.IntType,
	ResponseElapsedInMs:   xreflect.IntType,
	ResponseUnixTimeInSec: xreflect.IntType,
}
