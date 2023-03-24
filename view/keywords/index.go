package keywords

import (
	"github.com/viant/velty"
	"github.com/viant/velty/functions"
	"reflect"
)

type Namespace struct{}

func NewNamespace() *Namespace {
	return &Namespace{}
}

var registryInstance = functions.CopyInstance()

func AddAndGet(name string, entry *functions.Entry) string {
	return registryInstance.DefineNs(name, entry)
}

func Add(name string, entry *functions.Entry) {
	registryInstance.DefineNs(name, entry)
}
func Has(name string) bool {
	return registryInstance.IsDefined(name)
}

func RegisterType(rType reflect.Type) {
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	if rType.Kind() != reflect.Struct {
		return
	}

	numField := rType.NumField()
	for i := 0; i < numField; i++ {
		field := rType.Field(i)
		fieldTag := velty.Parse(field.Tag.Get("velty"))
		for _, name := range fieldTag.Names {
			Add(name, functions.NewEntry(
				nil,
				functions.NewFunctionNamespace(field.Type),
			))
		}
	}

}
