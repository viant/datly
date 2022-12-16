package expand

import (
	"fmt"
	"github.com/viant/structql"
	"github.com/viant/velty/ast/expr"
	"reflect"
)

const (
	queryFunctionName      = "Query"
	queryFirstFunctionName = "QueryFirst"
)

type (
	queryFunction      struct{}
	queryFirstFunction struct{}
)

func (q *queryFirstFunction) ResultType(receiver reflect.Type, call *expr.Call) (reflect.Type, error) {
	resultType, err := queryFnHandler.ResultType(receiver, call)
	if err != nil {
		return nil, err
	}

	return resultType.Elem().Elem(), nil
}

func (q *queryFirstFunction) Kind() []reflect.Kind {
	return []reflect.Kind{reflect.Ptr, reflect.Struct, reflect.Slice}
}

func (q *queryFirstFunction) Handler() interface{} {
	return func(data interface{}, query string) (interface{}, error) {
		fmt.Printf("%T, %v\n", data, data)
		result, err := queryFnHandler.handleQuery(data, query)
		if err != nil {
			return nil, err
		}

		rValue := reflect.ValueOf(result)
		if rValue.Len() == 0 {
			return NewValue(rValue.Type().Elem()), nil
		}

		result = rValue.Index(0).Interface()

		return result, nil
	}
}

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

//TODO cache queries
func (q *queryFunction) handleQuery(data interface{}, query string) (interface{}, error) {
	parsedQuery, err := structql.NewQuery(query, reflect.TypeOf(data), nil)
	if err != nil {
		return nil, err
	}

	result, err := parsedQuery.Select(data)
	if err != nil {
		return nil, err
	}

	return reflect.ValueOf(result).Elem().Interface(), nil
}

var queryFirstFnHandler = &queryFirstFunction{}
