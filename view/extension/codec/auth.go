package codec

import (
	"context"
	"fmt"
	"github.com/viant/scy/auth"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"reflect"
	"strconv"
	"time"
)

const (
	KeyCustomAuth = "CustomAuth"
)

type Authenticator interface {
	Authenticate(ctx context.Context, id, password string, expireIn time.Duration, claims string) (*auth.Token, error)
}

type customAuth struct {
	authenticator Authenticator
	codecConfig   *codec.Config
}

func (e *customAuth) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	if len(codecConfig.Args) < 2 {
		return nil, fmt.Errorf("3 arguments are required: subject, password")
	}
	return &customAuth{authenticator: e.authenticator, codecConfig: codecConfig}, nil
}

func (e *customAuth) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&auth.Token{}), nil
}

func (e *customAuth) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	token, err := e.authenticate(ctx, raw)
	return token, err
}

func (e *customAuth) authenticate(ctx context.Context, raw interface{}) (interface{}, error) {
	if raw == nil {
		return "", fmt.Errorf("raw was nil")
	}
	aType := structology.NewStateType(reflect.TypeOf(raw))
	state := aType.WithValue(raw)
	username, err := state.String(e.codecConfig.Args[0])
	if err != nil {
		return "", err
	}
	password, err := state.String(e.codecConfig.Args[1])
	var ttl = 60 * 60 //1 hour
	if len(e.codecConfig.Args) > 2 {
		if ttl, err = strconv.Atoi(e.codecConfig.Args[2]); err != nil {
			return "", fmt.Errorf("invalid ttl: %w", err)
		}
	}
	var claims = "{}"
	if len(e.codecConfig.Args) > 3 {
		if claims = e.codecConfig.Args[3]; claims == "" {
			claims = "{}"
		}
	}
	duration := time.Duration(ttl) * time.Second
	if e.authenticator == nil {
		return "", fmt.Errorf("authenticator was not set")
	}
	token, err := e.authenticator.Authenticate(ctx, username, password, duration, claims)
	return token, err
}

// NewCustomAuth	create a new authenticator auth codec
func NewCustomAuth(authenticator Authenticator) codec.Factory {
	return &customAuth{authenticator: authenticator}
}
