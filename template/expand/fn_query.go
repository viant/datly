package expand

import (
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
	if err := checkArgsSize(call, 1); err != nil {
		return nil, err
	}

	asLiteral, ok := call.Args[0].(*expr.Literal)
	if !ok {
		return nil, unexpectedArgType(0, asLiteral, call)
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
