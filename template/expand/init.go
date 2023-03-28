package expand

import (
	"github.com/viant/datly/view/keywords"
	"reflect"
)

func init() {
	ctxType := reflect.TypeOf(Context{})
	keywords.RegisterType("", ctxType)
}
