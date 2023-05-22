package expand

import (
	"github.com/viant/velty"
	"os"
	"reflect"
)

var isDebugEnabled = os.Getenv("DATLY_NOPANIC") == ""

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
