package jwt

import (
	"context"
	"github.com/viant/scy/auth/jwt"
)

type (
	jwtVerifier struct {
		chain []verify
		cache *cache
	}

	verify func(ctx context.Context, rawToken string) (*jwt.Claims, error)
)

func (s *jwtVerifier) add(verify verify) {
	s.chain = append(s.chain, verify)
}

func (s *jwtVerifier) verifyToken(ctx context.Context, rawToken string) (*jwt.Claims, error) {
	claims := s.cache.get(rawToken)
	if claims != nil {
		return claims, nil
	}
	var err error
	for _, verify := range s.chain {
		if claims, err = verify(ctx, rawToken); err == nil {
			s.cache.set(rawToken, claims)
			return claims, nil
		}
	}

	return nil, err
}

func newJwtVerifier() *jwtVerifier {
	return &jwtVerifier{cache: newCache()}
}
