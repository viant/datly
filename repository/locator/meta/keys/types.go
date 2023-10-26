package keys

import (
	"github.com/viant/xreflect"
	"reflect"
)

var Types = map[string]reflect.Type{
	//component/View related keys
	ViewID:          xreflect.StringType,
	ViewName:        xreflect.StringType,
	ViewDescription: xreflect.StringType,
}
