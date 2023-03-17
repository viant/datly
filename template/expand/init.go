package expand

import (
	"github.com/viant/datly/view/keywords"
	"github.com/viant/velty"
	"github.com/viant/velty/functions"
	"reflect"
)

func init() {
	ctxType := reflect.TypeOf(Context{})
	numField := ctxType.NumField()

	for i := 0; i < numField; i++ {
		field := ctxType.Field(i)
		fieldTag := velty.Parse(field.Tag.Get("velty"))
		for _, name := range fieldTag.Names {
			keywords.Add(name, functions.NewEntry(
				nil,
				functions.NewFunctionNamespace(field.Type),
			))
		}
	}
}
