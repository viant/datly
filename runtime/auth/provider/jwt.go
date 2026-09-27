package provider

import (
	"context"
	"fmt"

	scyauth "github.com/viant/scy/auth"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	xauth "github.com/viant/xdatly/auth"
)

type jwtAuthenticator struct{ verifier *verifier.Service }

func (*jwtAuthenticator) BasicAuth(context.Context, string, string) (*scyauth.Token, error) {
	return nil, fmt.Errorf("%w: JWT does not support basic authentication", xauth.ErrUnsupportedVendor)
}
func (a *jwtAuthenticator) VerifyIdentity(ctx context.Context, token string) (*jwt.Claims, error) {
	return a.verifier.VerifyClaims(ctx, token)
}
func (*jwtAuthenticator) ReissueIdentityToken(context.Context, string, string) (*scyauth.Token, error) {
	return nil, fmt.Errorf("%w: JWT does not reissue identity tokens", xauth.ErrUnsupportedVendor)
}
func (*jwtAuthenticator) ResetCredentials(context.Context, string, string) error {
	return fmt.Errorf("%w: JWT does not reset credentials", xauth.ErrUnsupportedVendor)
}

var _ xauth.Authenticator = (*jwtAuthenticator)(nil)
