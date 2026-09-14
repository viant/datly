package http

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestOpenAPIConfigurationRoundTrip(t *testing.T) {
	var config Config
	require.NoError(t, yaml.Unmarshal([]byte(`Meta:
  OpenApiURI: /metadata/openapi
  DocURI: /docs
  CacheWarmURI: ' '
OpenAPI:
  Info:
    title: Configured API
    version: one
  AggregateAccess:
    APIKeyHeader: X-Docs
    APIKeyValue: secret
`), &config))
	require.Equal(t, "Configured API", config.OpenAPI.Info.Title)
	require.Equal(t, "/metadata/openapi", config.Meta.OpenApiURI)
	require.NoError(t, config.OpenAPI.AggregateAccess.Validate())
	raw, err := json.Marshal(config)
	require.NoError(t, err)
	var decoded Config
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, config, decoded)
	require.Equal(t, DefaultOpenAPIURI, (Config{}).resolved().Meta.OpenApiURI)
	require.Equal(t, DefaultDocURI, (Config{OpenAPI: &OpenAPIConfig{}}).resolved().Meta.DocURI)
	require.Equal(t, " ", (Config{OpenAPI: &OpenAPIConfig{}, Meta: Meta{DocURI: " "}}).resolved().Meta.DocURI)
	require.Empty(t, (Config{}).resolved().Meta.DocURI)
	require.Equal(t, " ", (Config{Meta: Meta{OpenApiURI: " "}}).resolved().Meta.OpenApiURI)
}
