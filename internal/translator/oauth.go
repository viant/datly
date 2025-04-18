package translator

import (
	"context"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/cognito"
	"github.com/viant/scy/auth/firebase"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
	"strings"
)

func (c *Config) updateAuth(ctx context.Context) error {
	cfg := c.Config.Config
	if res := c.repository.RSA; res != "" {
		privateRes := ""
		if idx := strings.Index(res, ";"); idx != -1 {
			privateRes = res[idx+1:]
			res = res[:idx]
		}

		c.ensureJWTValidator(cfg)
		cfg.JWTValidator.RSA = getScyResource(res)

		if cfg.JwtSigner == nil {
			cfg.JwtSigner = &signer.Config{}
		}
		if privateRes != "" {
			res = privateRes
		}
		cfg.JwtSigner.RSA = getScyResource(res)
	}

	if res := c.repository.HMAC; res != "" {
		c.ensureJWTValidator(cfg)
		cfg.JWTValidator.HMAC = getScyResource(res)
		if cfg.JwtSigner == nil {
			cfg.JwtSigner = &signer.Config{}
		}
		if cfg.JwtSigner.RSA == nil { //prioritize RSA over HMAC
			cfg.JwtSigner.HMAC = getScyResource(res)
		}
	}

	if res := c.repository.Firebase; res != "" {
		webAPIRes := ""
		if idx := strings.Index(res, ";"); idx != -1 {
			webAPIRes = res[idx+1:]
			res = res[:idx]
		}
		cfg.Firebase = &firebase.Config{
			Secrets: getScyResource(res),
		}
		if webAPIRes != "" {
			cfg.Firebase.WebAPIKey = getScyResource(webAPIRes)
		}
	}

	if res := c.repository.Cognito; res != "" {
		cfg.Cognito = &cognito.Config{
			Resource: getScyResource(res),
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
