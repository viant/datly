package handler

import (
	"fmt"
	"github.com/viant/datly/service/auth"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
	hauth "github.com/viant/xdatly/handler/auth"
)

type Auth struct {
	authService *auth.Service
}

func (a *Auth) Authenticator(vendor hauth.Vendor) (hauth.Authenticator, error) {
	switch vendor {
	case hauth.VendorFirebase:
		return a.authService.Firebase(), nil
	case hauth.VendorCognito:
		return a.authService.Cognito(), nil
	default:
		return nil, fmt.Errorf("unsupported vendor: %v", vendor)
	}
}

func (a *Auth) Signer() *signer.Service {
	return a.authService.Signer()
}
func (a *Auth) Verifier() *verifier.Service {
	return a.authService.Verifier()
}

func NewAuth(authService *auth.Service) hauth.Auth {
	return &Auth{
		authService: authService,
	}
}
