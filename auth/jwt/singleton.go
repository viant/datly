package jwt

import (
	"context"
	"embed"
	"github.com/viant/afs"
	"github.com/viant/datly/auth/cognito"
	"github.com/viant/datly/codec"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/registry"
	"github.com/viant/scy/auth/jwt/verifier"
	"sync"
)

var cognitoService *cognito.Service
var jwtVerifier *verifier.Service
var authServiceInit sync.Once

func Init(config *gateway.Config, embedFs *embed.FS) (Authenticator, error) {
	fs := afs.New()
	var err error
	authServiceInit.Do(func() {
		if config.Cognito != nil {
			if embedFs == nil { //default FS
				embedFs = &cognito.EmbedFs
			}
			if cognitoService, err = cognito.New(config.Cognito, fs, embedFs); err == nil {
				provider := New(cognitoService.VerifyIdentity)
				registry.Codecs.Register(codec.NewVisitor(registry.CodecKeyJwtClaim, provider))
				registry.Codecs.Register(codec.NewVisitor(registry.CodecCognitoKeyJwtClaim, provider))
			}
		}
		if config.JWTValidator != nil {
			jwtVerifier = verifier.New(config.JWTValidator)
			if err = jwtVerifier.Init(context.Background()); err == nil {
				registry.Codecs.Register(codec.NewVisitor(registry.CodecKeyJwtClaim, New(jwtVerifier.VerifyClaims)))
			}
		}
	})
	if err != nil {
		authServiceInit = sync.Once{}
		cognitoService = nil
		return nil, err
	}
	if cognitoService == nil {
		return nil, nil
	}
	return cognitoService, nil
}
