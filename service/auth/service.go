package auth

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/service/auth/cognito"
	"github.com/viant/datly/service/auth/config"
	"github.com/viant/datly/service/auth/firebase"
	"github.com/viant/datly/service/auth/jwt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	dcodec "github.com/viant/datly/view/extension/codec"
	"github.com/viant/scy/auth/custom"
	"github.com/viant/scy/auth/gcp"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
	"path"
	"time"
)

type Service struct {
	cognitoAuth     *cognito.Service
	firebaseAuth    *firebase.Service
	verifierService *verifier.Service
	signerService   *signer.Service
	custom          *custom.Service
	config          *config.Config
}

func (s *Service) Verifier() *verifier.Service {
	return s.verifierService
}

func (s *Service) Signer() *signer.Service {
	return s.signerService
}

func (s *Service) Cognito() *cognito.Service {
	return s.cognitoAuth
}

func (s *Service) Firebase() *firebase.Service {
	return s.firebaseAuth
}

func (s *Service) Custom() *custom.Service {
	return s.custom
}

func (s *Service) Init(ctx context.Context) error {

	jwtTokenVerifier := jwt.NewJwtVerifier()
	jwtTokenChainVerifier := jwt.NewJwtVerifier()

	if s.config.JWTValidator != nil {
		s.verifierService = verifier.New(s.config.JWTValidator)
		if err := s.verifierService.Init(ctx); err != nil {
			return err
		}
		jwtTokenVerifier.AddIfEmpty(s.verifierService.VerifyClaims)
		jwtTokenChainVerifier.AddIfEmpty(s.verifierService.VerifyClaims)
	}

	if s.config.JwtSigner != nil {
		s.signerService = signer.New(s.config.JwtSigner)
		if err := s.signerService.Init(ctx); err != nil {
			return err
		}
	}
	if s.config.Cognito != nil {
		var err error
		if s.cognitoAuth, err = cognito.New(s.config.Cognito); err != nil {
			return err
		}
		extension.Config.RegisterCodecFactory(dcodec.KeyCognitoAuth, dcodec.NewCogitoAuth(s.cognitoAuth), time.Time{})
		jwtTokenVerifier.AddIfEmpty(s.cognitoAuth.VerifyIdentity)
		jwtTokenChainVerifier.AddIfEmpty(s.cognitoAuth.VerifyIdentity)
	}

	if s.config.Firebase != nil {
		var err error
		if s.firebaseAuth, err = firebase.New(ctx, s.config.Firebase); err != nil {
			return err
		}
		extension.Config.RegisterCodecFactory(dcodec.KeyFirebaseAuth, dcodec.NewFirebaseAuth(s.firebaseAuth), time.Time{})
		jwtTokenVerifier.AddIfEmpty(s.firebaseAuth.VerifyIdentity)
		jwtTokenChainVerifier.AddIfEmpty(s.firebaseAuth.VerifyIdentity)
	}

	if customConfig := s.config.Custom; customConfig != nil {
		if s.config.DependencyURL == "" {
			return fmt.Errorf("dependencyURL was empty")
		}
		connectionURL := path.Join(s.config.DependencyURL, "connections.yaml")
		fs := afs.New()
		if ok, _ := fs.Exists(context.Background(), connectionURL); ok {
			var authConnector, identityConnector *view.Connector
			if resource, _ := view.LoadResourceFromURL(context.Background(), connectionURL, fs); resource != nil {
				if err := resource.Init(context.Background()); err == nil {
					authConnector, _ = resource.Connector(customConfig.AuthConnector)
				}
				if customConfig.IdentityConnector != "" {
					identityConnector, _ = resource.Connector(customConfig.IdentityConnector)
				}
				s.custom = custom.New(customConfig, authConnector, identityConnector, s.signerService)
				extension.Config.RegisterCodecFactory(dcodec.KeyCustomAuth, dcodec.NewCustomAuth(s.custom), time.Time{})
			}
		}

	}

	if jwtTokenChainVerifier.Size() == 0 {
		jwtTokenVerifier.Add(gcp.JwtClaims)
		jwtTokenChainVerifier.Add(gcp.JwtClaims)
	}
	extension.Config.RegisterCodec(extension.CodecKeyJwtClaim, jwt.New(jwtTokenVerifier.VerifyToken), time.Time{})
	extension.Config.RegisterCodec(extension.CodecKeyChainJwtClaim, jwt.New(jwtTokenChainVerifier.VerifyToken), time.Time{})

	return nil
}

func New(config *config.Config) *Service {
	return &Service{config: config}
}