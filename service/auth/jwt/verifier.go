package jwt

import (
	"context"
	"github.com/viant/scy/auth/jwt"
)

type (
	JwtChainVerifier struct {
		chain []verify
		cache *cache
	}

	verify func(ctx context.Context, rawToken string) (*jwt.Claims, error)
)

func (s *JwtChainVerifier) Size() int {
	return len(s.chain)
}

func (s *JwtChainVerifier) AddIfEmpty(verify verify) {
	if len(s.chain) == 0 {
		s.Add(verify)
	}
}

func (s *JwtChainVerifier) Add(verify verify) {
	s.chain = append(s.chain, verify)
}

func (s *JwtChainVerifier) VerifyToken(ctx context.Context, rawToken string) (*jwt.Claims, error) {
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

func NewJwtVerifier() *JwtChainVerifier {
	return &JwtChainVerifier{cache: newCache()}
}
