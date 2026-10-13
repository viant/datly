package expand

import (
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/functions"
	"github.com/viant/xreflect"
	"reflect"
)

var fnNew = keywords.AddAndGet("New", &functions.Entry{
	Metadata: &keywords.StandaloneFn{},
	Handler:  nil,
})

type newer struct {
	lookup xreflect.LookupType
}

func (n *newer) New(aType string) (interface{}, error) {
	parseType, err := types.LookupType(n.lookup, aType)
	if err != nil {
		return nil, err
	}

	resultType := parseType
	if resultType.Kind() == reflect.Ptr {
		return reflect.New(resultType.Elem()).Interface(), nil
	}

	return reflect.New(resultType).Elem().Interface(), nil

}

func (n *newer) NewResultType(call *expr.Call) (reflect.Type, error) {
	if len(call.Args) != 1 {
		return nil, fmt.Errorf("expected New method to be called with 1 arg but was called with %v", len(call.Args))
	}

	expression, ok := call.Args[0].(*expr.Literal)
	if !ok {
		return nil, fmt.Errorf("expected arg to be type of %T but was %T", expression, call.Args[0])
	}

	return types.LookupType(n.lookup, expression.Value)
}
