package expand

import (
	"github.com/viant/datly/view/keywords"
	"github.com/viant/velty"
	"reflect"
)

func init() {
	ctxType := reflect.TypeOf(Context{})
	numField := ctxType.NumField()

	for i := 0; i < numField; i++ {
		fieldTag := velty.Parse(ctxType.Field(i).Tag.Get("velty"))
		for _, name := range fieldTag.Names {
			keywords.ReservedKeywords.Add(name)
		}
	}
}
