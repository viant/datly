package command

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway"
)

func TestApplyAuth_RSAUsesSemicolonSeparatedKeys(t *testing.T) {
	cfg := &gateway.Config{}
	auth := &options.Auth{
		RSA: "./github.com/viant-internal/public.pem|pubKey;./github.com/viant-internal/private.pem|privKey",
	}

	applyAuth(cfg, auth)

	require.NotNil(t, cfg.JWTValidator)
	require.Len(t, cfg.JWTValidator.RSA, 1)
	assert.True(t, strings.HasSuffix(cfg.JWTValidator.RSA[0].URL, "/github.com/viant-internal/public.pem"), cfg.JWTValidator.RSA[0].URL)
	assert.Equal(t, "pubKey", cfg.JWTValidator.RSA[0].Key)
	require.NotNil(t, cfg.JwtSigner)
	require.NotNil(t, cfg.JwtSigner.RSA)
	assert.True(t, strings.HasSuffix(cfg.JwtSigner.RSA.URL, "/github.com/viant-internal/private.pem"), cfg.JwtSigner.RSA.URL)
	assert.Equal(t, "privKey", cfg.JwtSigner.RSA.Key)
}

func TestGetScyResources_PreservesHyphenatedPaths(t *testing.T) {
	resources := getScyResources("./github.com/viant-internal/public.pem|pubKey")
	require.Len(t, resources, 1)
	assert.True(t, strings.HasSuffix(resources[0].URL, "/github.com/viant-internal/public.pem"), resources[0].URL)
	assert.Equal(t, "pubKey", resources[0].Key)
}
