package expand

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/xreflect"
	"io/ioutil"
	"net/http"
	"reflect"
)

const fnTransform = "TransformWithURL"

type (
	transform struct {
		types      map[string]reflect.Type
		typeLookup xreflect.LookupType
	}

	typesIndex map[string]reflect.Type
)

func (i typesIndex) Lookup(path string, identifier string, name string) (reflect.Type, bool) {
	rType, ok := i[name]
	return rType, ok
}

func newTransform(typeLookup xreflect.LookupType) *transform {
	return &transform{typeLookup: typeLookup, types: map[string]reflect.Type{}}
}

func (t *transform) ResultType(_ reflect.Type, call *expr.Call) (reflect.Type, error) {
	if err := checkArgsSize(call, 2); err != nil {
		return nil, err
	}

	asLiteral, ok := call.Args[1].(*expr.Literal)
	if !ok {
		return nil, unexpectedArgType(1, asLiteral, call.Args[1])
	}

	return t.parseType(asLiteral.Value)
}

func (t *transform) parseType(dataType string) (reflect.Type, error) {
	if rType, ok := t.types[dataType]; ok {
		return rType, nil
	}

	parsed, err := xreflect.Parse(dataType, xreflect.WithTypeLookup(t.typeLookup))
	if err != nil {
		return nil, err
	}

	t.types[dataType] = parsed
	return parsed, nil
}

func (t *transform) Kind() []reflect.Kind {
	kinds := make([]reflect.Kind, reflect.Struct-reflect.Bool+1)
	for i := 0; i < len(kinds); i++ {
		kinds[i] = reflect.Kind(int(reflect.Bool) + i)
	}
	return kinds
}

func (t *transform) Handler() interface{} {
	return func(data interface{}, url string, toType string) (interface{}, error) {
		body, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}

		request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}

		response, err := (&http.Client{}).Do(request)
		if err != nil {
			return nil, err
		}

		if response.StatusCode >= 300 || response.StatusCode < 200 {
			return nil, fmt.Errorf("got error status code while transforming data")
		}

		content, err := ioutil.ReadAll(response.Body)
		defer response.Body.Close()
		if err != nil {
			return nil, err
		}

		parseType, err := t.parseType(toType)
		if err != nil {
			return nil, err
		}

		result := reflect.New(parseType)

		if len(content) != 0 {
			if err = json.Unmarshal(content, result.Interface()); err != nil {
				return nil, err
			}
		}

		return result.Elem().Interface(), nil
	}
}
