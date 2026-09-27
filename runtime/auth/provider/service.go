// Package provider composes statically linked authentication implementations.
package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/viant/scy/auth/cognito"
	"github.com/viant/scy/auth/firebase"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
	xauth "github.com/viant/xdatly/auth"
)

// Config is trusted host configuration. Product database access is deliberately
// absent: the default authenticator is supplied by the application and invokes
// generated Datly readers and writers.
type Config struct {
	Default      xauth.Authenticator
	JWTValidator *verifier.Config
	JWTSigner    *signer.Config
	Cognito      *cognito.Config
	Firebase     *firebase.Config
}

// Service is the immutable provider bound to custom handlers.
type Service struct {
	authenticators map[xauth.Vendor]xauth.Authenticator
	signer         *signer.Service
	verifier       *verifier.Service
}

func (s *Service) Authenticator(vendor xauth.Vendor) (xauth.Authenticator, error) {
	if vendor == "" {
		vendor = xauth.VendorDefault
	}
	if s == nil {
		return nil, fmt.Errorf("%w: %s", xauth.ErrUnsupportedVendor, vendor)
	}
	authenticator := s.authenticators[vendor]
	if authenticator == nil {
		return nil, fmt.Errorf("%w: %s", xauth.ErrUnsupportedVendor, vendor)
	}
	return authenticator, nil
}

func (s *Service) Signer() *signer.Service {
	if s == nil {
		return nil
	}
	return s.signer
}

func (s *Service) Verifier() *verifier.Service {
	if s == nil {
		return nil
	}
	return s.verifier
}

// New constructs every configured implementation before the application is
// allowed to serve. Missing default/JWT services fail closed.
func New(ctx context.Context, config Config) (*Service, error) {
	if ctx == nil {
		return nil, errors.New("auth provider context is required")
	}
	if config.Default == nil || config.JWTSigner == nil || config.JWTValidator == nil {
		return nil, errors.New("auth provider requires default authenticator, JWT signer, and JWT verifier")
	}
	result := &Service{authenticators: map[xauth.Vendor]xauth.Authenticator{xauth.VendorDefault: config.Default}}
	result.signer = signer.New(config.JWTSigner)
	if err := result.signer.Init(ctx); err != nil {
		return nil, fmt.Errorf("initialize JWT signer: %w", err)
	}
	result.verifier = verifier.New(config.JWTValidator)
	if err := result.verifier.Init(ctx); err != nil {
		return nil, fmt.Errorf("initialize JWT verifier: %w", err)
	}
	result.authenticators[xauth.VendorJWT] = &jwtAuthenticator{verifier: result.verifier}
	if config.Cognito != nil {
		service, err := cognito.New(ctx, config.Cognito)
		if err != nil {
			return nil, fmt.Errorf("initialize Cognito authenticator: %w", err)
		}
		result.authenticators[xauth.VendorCognito] = &cognitoAuthenticator{service: service}
	}
	if config.Firebase != nil {
		service, err := firebase.New(ctx, config.Firebase)
		if err != nil {
			return nil, fmt.Errorf("initialize Firebase authenticator: %w", err)
		}
		result.authenticators[xauth.VendorFirebase] = &firebaseAuthenticator{service: service}
	}
	return result, nil
}

var _ xauth.Auth = (*Service)(nil)
