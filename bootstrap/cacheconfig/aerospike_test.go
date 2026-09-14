package cacheconfig

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestAerospikeConfigurationFailsBeforeOpeningUnownedClient(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*spec.CacheSettings)
		want   string
	}{
		{"pool_required", func(*spec.CacheSettings) {}, "client pool is required"},
		{"fractional_ttl", func(s *spec.CacheSettings) { s.TTL = "1500ms" }, "whole positive seconds"},
		{"socket_timeout", func(s *spec.CacheSettings) { s.SocketTimeoutInMs = -1 }, "socketTimeoutInMs"},
		{"reset_pair", func(s *spec.CacheSettings) { s.FailedRequestLimit = 1 }, "both be positive"},
		{"provider_namespace", func(s *spec.CacheSettings) { s.Provider = "aerospike://localhost:3000" }, "namespace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := &spec.CacheSettings{Enabled: true, Provider: "aerospike://localhost:3000/test", Location: "cache", TTL: "1m"}
			tc.change(settings)
			service, err := (Config{Identity: "component/root", Settings: settings}).New()
			if service != nil || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("service=%T,error=%v", service, err)
			}
		})
	}
}
