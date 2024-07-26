package jwt

import (
	"context"
	"embed"
	"github.com/viant/afs"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/service/auth/cognito"
	"github.com/viant/datly/service/auth/firebase"
	"github.com/viant/scy/auth/custom"

	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	dcodec "github.com/viant/datly/view/extension/codec"
	"github.com/viant/scy/auth/gcp"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
	"path"
	"sync"
	"time"
)

var cognitoService *cognito.Service
var verifierService *verifier.Service
var firebaseService *firebase.Service
var authServiceInit sync.Once

func Init(gatewayConfig *gateway.Config, embedFs *embed.FS) (gateway.Authorizer, error) {
	fs := afs.New()
	var err error
	authServiceInit.Do(func() {
		var signerService *signer.Service
		jwtTokenVerifier := newJwtVerifier()
		if gatewayConfig.Cognito != nil {
			if embedFs == nil { //default FS
				embedFs = &cognito.EmbedFs
			}
			if cognitoService, err = cognito.New(gatewayConfig.Cognito, fs, embedFs); err == nil {
				jwtTokenVerifier.add(cognitoService.VerifyIdentity)
			}
		}

		if gatewayConfig.JwtSigner != nil {
			signerService = signer.New(gatewayConfig.JwtSigner)
			if err := signerService.Init(context.Background()); err != nil {
				return
			}
		}

		if gatewayConfig.JWTValidator != nil {
			verifierService = verifier.New(gatewayConfig.JWTValidator)
			if err = verifierService.Init(context.Background()); err == nil {
				jwtTokenVerifier.add(verifierService.VerifyClaims)
			}
		}

		if gatewayConfig.Firebase != nil && err == nil {
			if firebaseService, err = firebase.New(context.Background(), gatewayConfig.Firebase); err == nil {
				jwtTokenVerifier.add(firebaseService.VerifyIdentity)
				extension.Config.RegisterCodecFactory(dcodec.KeyFirebaseAuth, dcodec.NewFirebaseAuth(firebaseService), time.Time{})
			}
		}

		if customConfig := gatewayConfig.Custom; customConfig != nil {
			connectionURL := path.Join(gatewayConfig.DependencyURL, "connections.yaml")
			if ok, _ := fs.Exists(context.Background(), connectionURL); ok {
				var authConnector, identityConnector *view.Connector
				if res, _ := view.LoadResourceFromURL(context.Background(), connectionURL, fs); res != nil {
					if _ = res.Init(context.Background()); res != nil {
						authConnector, _ = res.Connector(customConfig.AuthConnector)
					}
					if customConfig.IdentityConnector != "" {
						identityConnector, _ = res.Connector(customConfig.IdentityConnector)
					}
					customAuthCodec := custom.New(customConfig, authConnector, identityConnector, signerService)
					extension.Config.RegisterCodecFactory(dcodec.KeyCustomAuth, dcodec.NewCustomAuth(customAuthCodec), time.Time{})
				}
			}
		}

		if len(jwtTokenVerifier.chain) == 0 {
			jwtTokenVerifier.add(gcp.JwtClaims)
		}
		extension.Config.RegisterCodec(extension.CodecKeyJwtClaim, New(jwtTokenVerifier.verifyToken), time.Time{})
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
