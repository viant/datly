package expand

import (
	"github.com/viant/velty"
	"reflect"
)

var isDebugEnabled = true

func SetPanicOnError(value bool) {
	isDebugEnabled = value
}

type CustomContext struct {
	Type  reflect.Type
	Value interface{}
}

type config struct {
	valueTypes   []*CustomContext
	panicOnError velty.PanicOnError
}

func newConfig() *config {
	return &config{
		panicOnError: velty.PanicOnError(isDebugEnabled),
	}
}
