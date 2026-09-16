package config

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateJSONKeys(t *testing.T) {
	for _, document := range []string{
		`{}`, `{"a":{},"b":[]}`, `{"a":null,"b":false,"c":1e999}`,
		`{"a":{"name":"first"},"b":{"name":"second"}}`,
		`{"providers":[{"name":"first"},{"name":"second"}]}`,
	} {
		require.NoError(t, validateJSONKeys([]byte(document), nil), document)
	}
	for _, document := range []string{
		`{"a":1,"a":2}`, `{"a":{"b":1,"b":2}}`,
		`{"a":[{"b":1,"b":2}]}`, `{"a":1,"\u0061":2}`,
	} {
		require.ErrorContains(t, validateJSONKeys([]byte(document), nil), "duplicate JSON key", document)
	}
	for _, document := range []string{"", `{`, `{"a":}`, `[1,]`, `{"a":1]`, `{} {}`, `{"a":1} trailing`} {
		require.Error(t, validateJSONKeys([]byte(document), nil), document)
	}
	err := validateJSONKeys([]byte(`{"private-key":"secret","private-key":"other-secret"}`), nil)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "private")
	require.NotContains(t, err.Error(), "secret")
}

func TestValidateJSONKeysUsesDestinationFields(t *testing.T) {
	for _, document := range []string{
		`{"Caches":{},"caches":{}}`,
		`{"CacheProviders":[],"cacheproviders":[]}`,
		`{"Caches":{"shared":{"Enabled":false,"enabled":true}}}`,
		`{"CacheProviders":[{"Name":"first","name":"second"}]}`,
		`{"APIPrefix":"first","apiprefix":"second"}`,
	} {
		require.ErrorContains(t, validateJSONKeys([]byte(document), reflect.TypeFor[*Config]()), "duplicate JSON key", document)
	}
	for _, document := range []string{
		`{"Caches":{"Shared":{"Enabled":true},"shared":{"enabled":false}}}`,
		`{"CacheProviders":[{"Name":"first"},{"name":"second"}]}`,
		`{"APIPrefix":"only"}`,
	} {
		require.NoError(t, validateJSONKeys([]byte(document), reflect.TypeFor[*Config]()), document)
	}
}
