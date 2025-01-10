package firebase

import (
	"context"
	"github.com/viant/scy/auth"
	"github.com/viant/scy/auth/firebase"
	"github.com/viant/scy/auth/gcp"
	"github.com/viant/scy/auth/jwt"
	"google.golang.org/api/option"
)

type (
	Service struct {
		firebase *firebase.Service
	}
)

func (s *Service) BasicAuth(ctx context.Context, user string, password string) (*auth.Token, error) {
	return s.firebase.InitiateBasicAuth(ctx, user, password)
}

func (s *Service) ResetCredentials(ctx context.Context, email, newPassword string) error {
	return s.firebase.ResetCredentials(ctx, email, newPassword)
}

func (s *Service) VerifyIdentity(ctx context.Context, idToken string) (*jwt.Claims, error) {
	return s.verifyToken(ctx, idToken)
}

func (s *Service) ReissueIdentityToken(ctx context.Context, refreshToken string, subject string) (*auth.Token, error) {
	return s.firebase.ReissueIdentityToken(ctx, refreshToken, subject)
}

func (s *Service) verifyToken(ctx context.Context, idToken string) (*jwt.Claims, error) {
	if claims, err := gcp.JwtClaims(ctx, idToken); err == nil {
		return claims, nil
	}
	return s.firebase.VerifyIdentity(ctx, idToken)
}

func New(ctx context.Context, config *firebase.Config) (*Service, error) {
	var options []option.ClientOption
	firebaseService, err := firebase.New(ctx, config, options...)
	if err != nil {
		return nil, err
	}
	return &Service{
		firebase: firebaseService,
	}, nil
}
