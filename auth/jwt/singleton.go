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
	"time"
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
				config.Config.RegisterCodec(config.CodecKeyJwtClaim, New(cognitoService.VerifyIdentity), time.Time{})
				config.Config.RegisterCodec(config.CodecCognitoKeyJwtClaim, New(cognitoService.VerifyIdentity), time.Time{})
			}
		}
		if gwayConfig.JWTValidator != nil {
			jwtVerifier = verifier.New(gwayConfig.JWTValidator)
			if err = jwtVerifier.Init(context.Background()); err == nil {
				config.Config.RegisterCodec(
					config.CodecKeyJwtClaim, New(jwtVerifier.VerifyClaims), time.Time{},
				)
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
