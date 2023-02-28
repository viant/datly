package jwt

import (
	"context"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/scy/auth/jwt"
	"reflect"
	"strings"
)

type Provider struct {
	name         string
	jwtValidator func(ctx context.Context, rawString string) (*jwt.Claims, error)
	resultType   reflect.Type
}

func (s *Provider) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return s.resultType, nil
}

func (s *Provider) Name() string {
	return s.name
}

func (s *Provider) Valuer() config.Valuer {
	return s
}

func (s *Provider) Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error) {
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

//New creates a jwt claim validator
func New(name string, jwtValidator func(ctx context.Context, rawString string) (*jwt.Claims, error)) config.BasicCodec {
	return &Provider{
		jwtValidator: jwtValidator,
		name:         name,
		resultType:   reflect.TypeOf(&jwt.Claims{}),
	}
}
