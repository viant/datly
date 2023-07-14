package translator

import (
	"context"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt/verifier"
	"strings"
)

func (c *Config) updateOauth(ctx context.Context) {
	cfg := c.Config.Config
	if res := c.repository.RSA; res != "" {
		c.ensureJWTValidator(cfg)
		cfg.JWTValidator.RSA = getScyResource(res)
	}
	if res := c.repository.HMAC; res != "" {
		c.ensureJWTValidator(cfg)
		cfg.JWTValidator.HMAC = getScyResource(res)
	}
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
