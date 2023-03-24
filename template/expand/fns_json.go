package expand

import (
	"encoding/json"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/velty/functions"
	"reflect"
)

var fnsJSON = keywords.AddAndGet("json", functions.NewEntry(
	jsoner,
	functions.NewFunctionNamespace(reflect.TypeOf(jsoner)),
))

type JSONer struct{}

var jsoner = &JSONer{}

func (n *JSONer) Marshall(any interface{}) (string, error) {
	marshal, err := json.Marshal(any)
	return string(marshal), err
}
