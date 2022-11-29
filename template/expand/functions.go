package expand

import (
	"fmt"
	"github.com/viant/structql"
	"github.com/viant/velty/ast/expr"
	"reflect"
)

const queryFunctionName = "Query"

type (
	queryFunction struct{}
)

var queryFnHandler = &queryFunction{}

func (q *queryFunction) ResultType(receiver reflect.Type, call *expr.Call) (reflect.Type, error) {
	if len(call.Args) != 1 {
		return nil, fmt.Errorf("unexpected number of function %v arguments, expected 1 got %v", queryFunctionName, len(call.Args))
	}

	asLiteral, ok := call.Args[0].(*expr.Literal)
	if !ok {
		return nil, fmt.Errorf("unsupported arg[1] type, expected %T, got %T", asLiteral, call.Args[0])
	}

	query, err := structql.NewQuery(asLiteral.Value, receiver, nil)
	if err != nil {
		return nil, err
	}

	return query.Type(), nil
}

func (q *queryFunction) Kind() []reflect.Kind {
	return []reflect.Kind{reflect.Ptr, reflect.Struct, reflect.Slice}
}

func (q *queryFunction) Handler() interface{} {
	return q.handleQuery
}

func (q *queryFunction) handleQuery(data interface{}, query string) (interface{}, error) {
	parsedQuery, err := structql.NewQuery(query, reflect.TypeOf(data), nil)
	if err != nil {
		return nil, err
	}

	return parsedQuery.Select(data)
}
