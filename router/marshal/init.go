package marshal

import (
	"github.com/viant/datly/view/keywords"
	"reflect"
)

var ctxType = reflect.TypeOf(CustomContext{})

func init() {
	keywords.RegisterType(ctxType)
}
