package provider

import (
	"context"

	scyauth "github.com/viant/scy/auth"
	"github.com/viant/scy/auth/firebase"
	"github.com/viant/scy/auth/gcp"
	"github.com/viant/scy/auth/jwt"
	xauth "github.com/viant/xdatly/auth"
)

type firebaseAuthenticator struct{ service *firebase.Service }

func (a *firebaseAuthenticator) BasicAuth(ctx context.Context, user, password string) (*scyauth.Token, error) {
	return a.service.InitiateBasicAuth(ctx, user, password)
}
func (a *firebaseAuthenticator) VerifyIdentity(ctx context.Context, token string) (*jwt.Claims, error) {
	if claims, err := gcp.JwtClaims(ctx, token); err == nil {
		return claims, nil
	}
	return a.service.VerifyIdentity(ctx, token)
}
func (a *firebaseAuthenticator) ReissueIdentityToken(ctx context.Context, refreshToken, subject string) (*scyauth.Token, error) {
	return a.service.ReissueIdentityToken(ctx, refreshToken, subject)
}
func (a *firebaseAuthenticator) ResetCredentials(ctx context.Context, email, newPassword string) error {
	return a.service.ResetCredentials(ctx, email, newPassword)
}

var _ xauth.Authenticator = (*firebaseAuthenticator)(nil)
