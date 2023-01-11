package jwt

import (
	"context"
	"embed"
	"github.com/viant/afs"
	"github.com/viant/datly/auth/cognito"
	"github.com/viant/datly/gateway"
	"github.com/viant/scy/auth/jwt/verifier"
	"github.com/viant/xdatly"
	"sync"
)

var cognitoService *cognito.Service
var jwtVerifier *verifier.Service
var authServiceInit sync.Once

func Init(config *gateway.Config, embedFs *embed.FS) (gateway.Authorizer, error) {
	fs := afs.New()
	var err error
	authServiceInit.Do(func() {
		if config.Cognito != nil {
			if embedFs == nil { //default FS
				embedFs = &cognito.EmbedFs
			}
			if cognitoService, err = cognito.New(config.Cognito, fs, embedFs); err == nil {
				provider := New(cognitoService.VerifyIdentity)
				xdatly.Config.RegisterCodec(xdatly.NewVisitor(xdatly.CodecKeyJwtClaim, provider))
				xdatly.Config.RegisterCodec(xdatly.NewVisitor(xdatly.CodecCognitoKeyJwtClaim, provider))
			}
		}
		if config.JWTValidator != nil {
			jwtVerifier = verifier.New(config.JWTValidator)
			if err = jwtVerifier.Init(context.Background()); err == nil {
				xdatly.Config.RegisterCodec(xdatly.NewVisitor(xdatly.CodecKeyJwtClaim, New(jwtVerifier.VerifyClaims)))
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
