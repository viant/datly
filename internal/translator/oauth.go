package translator

import (
	"context"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/cognito"
	custom "github.com/viant/scy/auth/custom"
	"github.com/viant/scy/auth/firebase"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
	"strings"
)

func (c *Config) updateAuth(ctx context.Context) error {
	cfg := c.Config.Config
	if res := c.repository.RSA; res != "" {
		c.ensureJWTValidator(cfg)
		cfg.JWTValidator.RSA = getScyResource(res)
	}

	if res := c.repository.HMAC; res != "" {
		c.ensureJWTValidator(cfg)
		cfg.JWTValidator.HMAC = getScyResource(res)
		if cfg.JwtSigner == nil {
			cfg.JwtSigner = &signer.Config{}
		}
		cfg.JwtSigner.HMAC = getScyResource(res)
	}

	if res := c.repository.Firebase; res != "" {
		cfg.Firebase = &firebase.Config{
			WebAPIKey: getScyResource(res),
		}
	}

	if res := c.repository.Cognito; res != "" {
		parts := strings.Split(res, "|")
		if len(parts) != 2 {
			return fmt.Errorf("invalid cognito auth resource: %v, expected poolID|secret", res)
		}
		cfg.Cognito = &cognito.Config{
			PoolID:   parts[0],
			Resource: getScyResource(parts[1]),
		}
	}

	if customOpts := c.repository.Custom; customOpts != "" {
		size := customOpts.Size()
		if size < 0 {
			return fmt.Errorf("invalid customOpts auth resource: %v", customOpts)
		}
		authConnector := customOpts.ShiftString()
		authQuery := customOpts.ShiftString()
		subjectConnector := customOpts.ShiftString()
		subjectQuery := customOpts.ShiftString()
		maxAttempts, _ := customOpts.ShiftInt()
		if maxAttempts < 1 {
			maxAttempts = 5
		}
		cfg.Custom = &custom.Config{
			AuthConnector:     authConnector,
			AuthSQL:           authQuery,
			IdentityConnector: subjectConnector,
			IdentitySQL:       subjectQuery,
			MaxAttempts:       maxAttempts,
		}
	}
	return nil
}

func (c *Config) ensureJWTValidator(cfg *gateway.Config) {
	if cfg.JWTValidator == nil {
		cfg.JWTValidator = &verifier.Config{}
	}
}

func getScyResource(location string) *scy.Resource {
	pair := strings.Split(location, "|")
	var result *scy.Resource
	switch len(pair) {
	case 2:
		result = &scy.Resource{URL: pair[0], Key: pair[1]}
	default:
		result = &scy.Resource{URL: pair[0]}
	}
	result.URL = url.Normalize(result.URL, file.Scheme)
	return result
}
