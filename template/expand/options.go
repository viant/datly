package expand

import (
	"github.com/viant/velty"
	"reflect"
)

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
		panicOnError: true,
	}
}
