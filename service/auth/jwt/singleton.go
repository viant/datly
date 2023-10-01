package jwt

import (
	"context"
	"embed"
	"github.com/viant/afs"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/repository/extension"
	cognito2 "github.com/viant/datly/service/auth/cognito"
	"github.com/viant/scy/auth/jwt/verifier"
	"sync"
	"time"
)

var cognitoService *cognito2.Service
var jwtVerifier *verifier.Service
var authServiceInit sync.Once

func Init(gwayConfig *gateway.Config, embedFs *embed.FS) (gateway.Authorizer, error) {
	fs := afs.New()
	var err error
	authServiceInit.Do(func() {
		if gwayConfig.Cognito != nil {
			if embedFs == nil { //default FS
				embedFs = &cognito2.EmbedFs
			}
			if cognitoService, err = cognito2.New(gwayConfig.Cognito, fs, embedFs); err == nil {
				extension.Config.RegisterCodec(extension.CodecKeyJwtClaim, New(cognitoService.VerifyIdentity), time.Time{})
				extension.Config.RegisterCodec(extension.CodecCognitoKeyJwtClaim, New(cognitoService.VerifyIdentity), time.Time{})
			}
		}
		if gwayConfig.JWTValidator != nil {
			jwtVerifier = verifier.New(gwayConfig.JWTValidator)
			if err = jwtVerifier.Init(context.Background()); err == nil {
				extension.Config.RegisterCodec(
					extension.CodecKeyJwtClaim, New(jwtVerifier.VerifyClaims), time.Time{},
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
