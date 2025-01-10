package cognito

import (
	"context"
	"github.com/viant/scy/auth"
	"github.com/viant/scy/auth/cognito"
	"github.com/viant/scy/auth/jwt"
)

type Service struct {
	config  *cognito.Config
	cognito *cognito.Service
}

func (s *Service) BasicAuth(ctx context.Context, user string, password string) (*auth.Token, error) {
	return s.cognito.InitiateBasicAuth(user, password)
}

func (s *Service) ResetCredentials(ctx context.Context, email, newPassword string) error {
	return s.cognito.ResetCredentials(email, newPassword)
}

func (s *Service) ReissueIdentityToken(ctx context.Context, refreshToken string, subject string) (*auth.Token, error) {
	return s.cognito.ReissueIdentityToken(ctx, refreshToken, subject)
}

func (s *Service) VerifyIdentity(ctx context.Context, idToken string) (*jwt.Claims, error) {
	return s.cognito.VerifyIdentity(ctx, idToken)
}

func New(config *cognito.Config) (*Service, error) {
	cognito, err := cognito.New(context.Background(), config)
	if err != nil {
		return nil, err
	}
	return &Service{
		config:  config,
		cognito: cognito,
	}, nil
}
