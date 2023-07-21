package expand

import (
	"github.com/viant/datly/view/keywords"
	"github.com/viant/velty"
	"github.com/viant/xreflect"
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
	valueTypes    []*CustomContext
	panicOnError  velty.PanicOnError
	constUpdaters []ConstUpdater
	typeLookup    xreflect.LookupType
	pSchema       reflect.Type
	hasSchema     reflect.Type
	stateName     string
	predicates    []*PredicateConfig
}

func newConfig() *config {
	return &config{
		panicOnError: velty.PanicOnError(isDebugEnabled),
		stateName:    keywords.ParamsKey,
	}
}
