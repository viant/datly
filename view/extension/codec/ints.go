package codec

import (
	"context"
	"fmt"
	"github.com/viant/xdatly/codec"
	"reflect"
	"strconv"
	"strings"
)

const (
	KeyAsInts = "AsInts"
)

type AsInts struct {
}

func (i *AsInts) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf([]int{}), nil
}

func (i *AsInts) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	aString, ok := raw.(string)
	if !ok {
		return nil, UnexpectedValueType(aString, raw)
	}

	split := strings.Split(aString, ",")
	result := make([]int, len(split))

	var err error
	for index, segment := range split {
		result[index], err = strconv.Atoi(segment)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func UnexpectedValueType(wanted interface{}, got interface{}) error {
	return fmt.Errorf("unexpected parameter value type, wanted %T, got %T", wanted, got)
}
