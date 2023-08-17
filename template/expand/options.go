package expand

import (
	"fmt"
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

type Variable struct {
	Type  reflect.Type
	Value interface{}
}

func (v *Variable) Validate() error {
	actualType := reflect.TypeOf(v.Value)
	if actualType != v.Type {
		return fmt.Errorf("type missmatch, wanted %v got %v", actualType.String(), v.Type.String())
	}

	return nil
}

type NamedVariable struct {
	Variable
	Name string
}

func (v *NamedVariable) New(value interface{}) *NamedVariable {
	newValue := *v
	newValue.Value = value
	return &newValue
}

type config struct {
	namedVariables []*NamedVariable
	embededTypes   []*Variable
	panicOnError   velty.PanicOnError
	constUpdaters  []ConstUpdater
	typeLookup     xreflect.LookupType
	pSchema        reflect.Type
	hasSchema      reflect.Type
	stateName      string
	predicates     []*PredicateConfig
}

func newConfig() *config {
	return &config{
		panicOnError: velty.PanicOnError(isDebugEnabled),
		stateName:    keywords.ParamsKey,
	}
}
