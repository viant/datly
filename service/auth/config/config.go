package config

import (
	"github.com/viant/datly/service/auth/secret"
	"github.com/viant/scy/auth/cognito"
	"github.com/viant/scy/auth/custom"
	"github.com/viant/scy/auth/firebase"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
)

type (
	Config struct {
		Secrets       []*secret.Resource
		JWTValidator  *verifier.Config
		JwtSigner     *signer.Config
		Cognito       *cognito.Config
		Firebase      *firebase.Config
		Custom        *custom.Config
		DependencyURL string
	}
)
