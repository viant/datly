package translator

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/runtime/standalone"
)

func TestConfig_updateAuth_RSAUsesSemicolonSeparatedKeys(t *testing.T) {
	cfg := &Config{
		repository: &options.Repository{
			Auth: options.Auth{
				RSA: "./github.com/viant-internal/public.pem|pubKey;./github.com/viant-internal/private.pem|privKey",
			},
		},
		Config: &standalone.Config{Config: &gateway.Config{}},
	}

	err := cfg.updateAuth(context.Background())
	require.NoError(t, err)
	require.NotNil(t, cfg.Config.Config.JWTValidator)
	require.Len(t, cfg.Config.Config.JWTValidator.RSA, 1)
	assert.True(t, strings.HasSuffix(cfg.Config.Config.JWTValidator.RSA[0].URL, "/github.com/viant-internal/public.pem"), cfg.Config.Config.JWTValidator.RSA[0].URL)
	assert.Equal(t, "pubKey", cfg.Config.Config.JWTValidator.RSA[0].Key)
	require.NotNil(t, cfg.Config.Config.JwtSigner)
	require.NotNil(t, cfg.Config.Config.JwtSigner.RSA)
	assert.True(t, strings.HasSuffix(cfg.Config.Config.JwtSigner.RSA.URL, "/github.com/viant-internal/private.pem"), cfg.Config.Config.JwtSigner.RSA.URL)
	assert.Equal(t, "privKey", cfg.Config.Config.JwtSigner.RSA.Key)
}

func TestGetScyResources_PreservesHyphenatedPaths(t *testing.T) {
	resources := getScyResources("./github.com/viant-internal/public.pem|pubKey")
	require.Len(t, resources, 1)
	assert.True(t, strings.HasSuffix(resources[0].URL, "/github.com/viant-internal/public.pem"), resources[0].URL)
	assert.Equal(t, "pubKey", resources[0].Key)
}
