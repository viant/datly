package codec

import (
	"context"
	"fmt"
	"github.com/viant/scy/auth"
	"github.com/viant/scy/auth/custom"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"reflect"
	"strconv"
	"time"
)

const (
	KeyCustomAuth = "customAuth"
)

type customAuth struct {
	custom      *custom.Service
	codecConfig *codec.Config
}

func (e *customAuth) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	if len(codecConfig.Args) < 4 {
		return nil, fmt.Errorf("4 arguments are required: subject, password, ttl, claims")
	}
	return &customAuth{custom: e.custom, codecConfig: codecConfig}, nil
}

func (e *customAuth) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&auth.Token{}), nil
}

func (e *customAuth) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	if raw == nil {
		return nil, nil
	}
	aType := structology.NewStateType(reflect.TypeOf(raw))
	state := aType.WithValue(raw)
	username, err := state.String(e.codecConfig.Args[0])
	if err != nil {
		return nil, err
	}
	password, err := state.String(e.codecConfig.Args[1])
	ttl, err := strconv.Atoi(e.codecConfig.Args[2])
	if err != nil {
		return nil, err
	}
	claims := e.codecConfig.Args[3]
	if claims == "" {
		claims = "{}"
	}
	duration := time.Duration(ttl) * time.Second
	return e.custom.Authenticate(ctx, username, password, duration, claims)
}

// NewCustomAuth	create a new custom auth codec
func NewCustomAuth(custom *custom.Service) codec.Factory {
	return &customAuth{custom: custom}
}
