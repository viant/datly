package auth

import (
	"github.com/viant/datly/service/auth/cognito"
	"github.com/viant/datly/service/auth/firebase"
	"github.com/viant/datly/service/auth/secret"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
)

type Config struct {
	Secrets      []*secret.Resource
	JWTValidator *verifier.Config
	JwtSigner    *signer.Config
	Cognito      *cognito.Config
	Firebase     *firebase.Config
}
