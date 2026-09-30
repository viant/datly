package provider

import (
	"context"

	scyauth "github.com/viant/scy/auth"
	"github.com/viant/scy/auth/cognito"
	"github.com/viant/scy/auth/jwt"
	xauth "github.com/viant/xdatly/auth"
)

type cognitoAuthenticator struct{ service *cognito.Service }

func (a *cognitoAuthenticator) BasicAuth(_ context.Context, user, password string) (*scyauth.Token, error) {
	return a.service.InitiateBasicAuth(user, password)
}
func (a *cognitoAuthenticator) VerifyIdentity(ctx context.Context, token string) (*jwt.Claims, error) {
	return a.service.VerifyIdentity(ctx, token)
}
func (a *cognitoAuthenticator) ReissueIdentityToken(ctx context.Context, refreshToken, subject string) (*scyauth.Token, error) {
	return a.service.ReissueIdentityToken(ctx, refreshToken, subject)
}
func (a *cognitoAuthenticator) ResetCredentials(_ context.Context, email, newPassword string) error {
	return a.service.ResetCredentials(email, newPassword)
}

var _ xauth.Authenticator = (*cognitoAuthenticator)(nil)
