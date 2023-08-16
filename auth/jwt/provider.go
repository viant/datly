package jwt

import (
	"context"
	"fmt"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/xdatly/codec"
	"reflect"
	"strings"
)

type Provider struct {
	jwtValidator func(ctx context.Context, rawString string) (*jwt.Claims, error)
	resultType   reflect.Type
}

func (s *Provider) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return s.resultType, nil
}

func (s *Provider) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	rawString, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("expected to got string but got %T", raw)
	}

	if index := strings.Index(rawString, " "); index != -1 {
		rawString = rawString[index+1:]
	}
	claims, err := s.jwtValidator(ctx, rawString)
	return claims, err
}

// New creates a jwt claim validator
func New(jwtValidator func(ctx context.Context, rawString string) (*jwt.Claims, error)) codec.Instance {
	return &Provider{
		jwtValidator: jwtValidator,
		resultType:   reflect.TypeOf(&jwt.Claims{}),
	}
}
