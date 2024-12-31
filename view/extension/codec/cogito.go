package codec

import (
	"context"
	"fmt"
	scognito "github.com/viant/scy/auth/cognito"

	"github.com/viant/datly/service/auth/cognito"
	"github.com/viant/scy/auth"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"reflect"
	"sync"
)

const (
	KeyCognitoAuth = "CognitoAuth"
)

type CogitoAuth struct {
	codecConfig *codec.Config
	cognitoAuth *cognito.Service
	sync.Mutex
}

func (e *CogitoAuth) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	if len(codecConfig.Args) < 2 {
		return nil, fmt.Errorf("at least 2 arguments are required")
	}
	var err error
	if e.cognitoAuth == nil {
		if e.cognitoAuth, err = cognito.New(&scognito.Config{}); err != nil {
			return nil, err
		}
	}
	return &CogitoAuth{codecConfig: codecConfig, cognitoAuth: e.cognitoAuth}, nil
}

func (e *CogitoAuth) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&auth.Token{}), nil
}

func (e *CogitoAuth) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
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
	if err != nil {
		return nil, err
	}

	token, err := e.cognitoAuth.BasicAuth(ctx, username, password)
	if err != nil {
		return nil, obfuscateCognitoErrorMessage(err)
	}
	return token, err
}

func obfuscateCognitoErrorMessage(err error) error {
	return err
}

func NewCogitoAuth(cognitoAuth *cognito.Service) codec.Factory {
	return &CogitoAuth{cognitoAuth: cognitoAuth}
}
