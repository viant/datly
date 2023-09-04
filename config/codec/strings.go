package codec

import (
	"context"
	"fmt"
	"github.com/viant/xdatly/codec"
	"reflect"
	"strings"
)

const (
	KeyAsStrings = "AsStrings"
)

type AsStrings struct {
}

func (s *AsStrings) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf([]string{}), nil
}

func (s *AsStrings) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	expectedRaw, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("expected to get string but got %T", raw)
	}

	return strings.Split(expectedRaw, ","), nil
}
