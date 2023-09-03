package gateway

import (
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/view/keywords"
	"reflect"
)

func init() {
	keywords.RegisterType("Interceptor", reflect.TypeOf(router.InterceptorContext{}))
}
