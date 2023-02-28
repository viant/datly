package jwt

import (
	"context"
	"embed"
	"github.com/viant/afs"
	"github.com/viant/datly/auth/cognito"
	"github.com/viant/datly/config"
	"github.com/viant/datly/gateway"
	"github.com/viant/scy/auth/jwt/verifier"
	"sync"
)

var cognitoService *cognito.Service
var jwtVerifier *verifier.Service
var authServiceInit sync.Once

func Init(gwayConfig *gateway.Config, embedFs *embed.FS) (gateway.Authorizer, error) {
	fs := afs.New()
	var err error
	authServiceInit.Do(func() {
		if gwayConfig.Cognito != nil {
			if embedFs == nil { //default FS
				embedFs = &cognito.EmbedFs
			}
			if cognitoService, err = cognito.New(gwayConfig.Cognito, fs, embedFs); err == nil {
				config.Config.RegisterCodec(New(config.CodecKeyJwtClaim, cognitoService.VerifyIdentity))
				config.Config.RegisterCodec(New(config.CodecCognitoKeyJwtClaim, cognitoService.VerifyIdentity))
			}
		}
		if gwayConfig.JWTValidator != nil {
			jwtVerifier = verifier.New(gwayConfig.JWTValidator)
			if err = jwtVerifier.Init(context.Background()); err == nil {
				config.Config.RegisterCodec(
					New(config.CodecKeyJwtClaim, jwtVerifier.VerifyClaims))
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
