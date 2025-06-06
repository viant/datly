package expand

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/structology"
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
		return fmt.Errorf("type missmatch, wanted %s got %s", actualType.String(), v.Type.String())
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
	setLiterals    func(state *structology.State) error
	stateType      *structology.StateType
	typeLookup     xreflect.LookupType
	stateName      string
	predicates     []*PredicateConfig
	context        context.Context
}

func newConfig() *config {
	return &config{
		panicOnError: velty.PanicOnError(isDebugEnabled),
		stateName:    keywords.ParamsKey,
	}
}
