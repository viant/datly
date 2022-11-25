package jwt

import (
	"context"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/scy/auth/jwt"
	"strings"
)

type Provider struct {
	jwtValidator func(ctx context.Context, rawString string) (*jwt.Claims, error)
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
func New(jwtValidator func(ctx context.Context, rawString string) (*jwt.Claims, error)) view.Valuer {
	return &Provider{jwtValidator: jwtValidator}
}
