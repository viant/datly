package config_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	"gopkg.in/yaml.v3"
)

func TestLoaderNamedCaches(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		for _, legacy := range []bool{false, true} {
			t.Run(format+map[bool]string{true: "/legacy", false: "/native"}[legacy], func(t *testing.T) {
				root := t.TempDir()
				require.NoError(t, os.Mkdir(filepath.Join(root, "deps"), 0700))
				inline := `{"enabled":true,"provider":"afs","location":"inline","ttl":"1m"}`
				main := `{"GoBootstrap":{"Packages":["example.com/app"]},"DependencyURL":"deps","Caches":{"inline":` + inline + `}}`
				require.NoError(t, os.WriteFile(filepath.Join(root, "config.json"), []byte(main), 0600))
				require.NoError(t, os.WriteFile(filepath.Join(root, "deps/connections.json"), []byte(`{"Connectors":[{"Name":"main","Driver":"sqlite3","DSN":"retained"}]}`), 0600))
				body := `{"Caches":{"aerospike":{"Enabled":true,"Provider":"aerospike://localhost:3000/test","Location":"steward_${View.Name}","TimeToLiveMs":14400000}}}`
				if legacy {
					body = `{"CacheProviders":[{"Name":"aerospike","Provider":"aerospike://localhost:3000/test","Location":"steward_${View.Name}","TimeToLiveMs":14400000}],"ModTime":"0001-01-01T00:00:00Z"}`
				}
				if format == "yaml" {
					body = "Caches:\n  aerospike:\n    Enabled: true\n    Provider: aerospike://localhost:3000/test\n    Location: steward_${View.Name}\n    TimeToLiveMs: 14400000\n"
					if legacy {
						body = "CacheProviders:\n  - Name: aerospike\n    Provider: aerospike://localhost:3000/test\n    Location: steward_${View.Name}\n    TimeToLiveMs: 14400000\nModTime: \"0001-01-01T00:00:00Z\"\n"
					}
				}
				require.NoError(t, os.WriteFile(filepath.Join(root, "deps/cache."+format), []byte(body), 0600))
				cfg, err := (config.Loader{}).Load(context.Background(), filepath.Join(root, "config.json"))
				require.NoError(t, err)
				require.NoError(t, cfg.Validate())
				require.Len(t, cfg.Connectors, 1)
				require.Equal(t, "retained", cfg.Connectors[0].DSN)
				require.Len(t, cfg.Caches, 2)
				require.Empty(t, cfg.CacheProviders)
				require.True(t, cfg.Caches["aerospike"].Enabled)
				require.Equal(t, "aerospike", cfg.Caches["aerospike"].Name)
				require.Equal(t, "steward_${View.Name}", cfg.Caches["aerospike"].Location)
				require.Equal(t, 14400000, cfg.Caches["aerospike"].TimeToLiveMs)
				// Identical dependency definitions are allowed regardless of file order.
				require.NoError(t, os.WriteFile(filepath.Join(root, "deps/duplicate."+format), []byte(body), 0600))
				_, err = (config.Loader{}).Load(context.Background(), filepath.Join(root, "config.json"))
				require.NoError(t, err)
				conflict := `{"CacheProviders":[{"Name":"aerospike","Provider":"afs","Location":"other","TimeToLiveMs":14400000}]}`
				require.NoError(t, os.WriteFile(filepath.Join(root, "deps/conflict.json"), []byte(conflict), 0600))
				_, err = (config.Loader{}).Load(context.Background(), filepath.Join(root, "config.json"))
				require.ErrorContains(t, err, `conflicting definitions for cache "aerospike"`)
			})
		}
	}
}

func TestLoaderCacheConflictsAndValidation(t *testing.T) {
	const valid = `{"Name":"shared","Enabled":true,"Location":"cache","TTL":"1m"}`
	for _, tc := range []struct{ name, inline, dependency, want string }{
		{"identical", `"Caches":{"shared":` + valid + `}`, `{"Caches":{"shared":` + valid + `}}`, ""},
		{"duplicate inline cache name", `"Caches":{"shared":` + valid + `,"shared":{"Enabled":true,"Location":"other","TTL":"2m"}}`, `{}`, "duplicate JSON key"},
		{"duplicate inline block", `"Caches":{"shared":` + valid + `},"Caches":{"shared":{"Enabled":true,"Location":"other","TTL":"2m"}}`, `{}`, "duplicate JSON key"},
		{"duplicate dependency cache name", `"Caches":{}`, `{"Caches":{"shared":` + valid + `,"shared":{"Enabled":true,"Location":"other","TTL":"2m"}}}`, "duplicate JSON key"},
		{"duplicate dependency block", `"Caches":{}`, `{"Caches":{"shared":` + valid + `},"Caches":{"shared":{"Enabled":true,"Location":"other","TTL":"2m"}}}`, "duplicate JSON key"},
		{"escaped duplicate name", `"Caches":{"shared":` + valid + `,"\u0073hared":` + valid + `}`, `{}`, "duplicate JSON key"},
		{"duplicate provider block", `"CacheProviders":[` + valid + `],"CacheProviders":[]`, `{}`, "duplicate JSON key"},
		{"duplicate provider property", `"CacheProviders":[{"Name":"shared","Enabled":false,"Enabled":true}]`, `{}`, "duplicate JSON key"},
		{"map versus list", `"Caches":{"shared":` + valid + `}`, `{"CacheProviders":[{"Name":"shared","Location":"other","TTL":"1m"}]}`, "conflicting definitions"},
		{"duplicate list", `"CacheProviders":[` + valid + `,{"Name":"shared","Location":"other","TTL":"1m"}]`, `{}`, "conflicting definitions"},
		{"missing name", `"CacheProviders":[{"Location":"cache","TTL":"1m"}]`, `{}`, "cache name"},
		{"empty key", `"Caches":{"":` + valid + `}`, `{}`, "cache name"},
		{"name mismatch", `"Caches":{"other":` + valid + `}`, `{}`, "disagrees"},
		{"dependency name mismatch", `"Caches":{}`, `{"Caches":{"other":` + valid + `}}`, "disagrees"},
		{"nil settings", `"Caches":{"shared":null}`, `{}`, "settings are required"},
		{"nil provider", `"CacheProviders":[null]`, `{}`, "settings are required"},
		{"missing location", `"Caches":{"shared":{"Enabled":true,"TTL":"1m"}}`, `{}`, "location is required"},
		{"bad ttl", `"Caches":{"shared":{"Enabled":true,"Location":"cache","TTL":"later"}}`, `{}`, "positive duration"},
		{"conflicting ttl", `"Caches":{"shared":{"Enabled":true,"Location":"cache","TTL":"1m","TimeToLiveMs":1}}`, `{}`, "disagree"},
		{"unsupported provider", `"Caches":{"shared":{"Enabled":true,"Location":"cache","TTL":"1m","Provider":"unknown"}}`, `{}`, "not supported"},
		{"disabled legacy", `"CacheProviders":[{"Name":"shared","Enabled":false}]`, `{}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(root, "cache.json"), []byte(tc.dependency), 0600))
			main := `{"GoBootstrap":{"Packages":["example.com/app"]},"DependencyURL":"cache.json",` + tc.inline + `}`
			require.NoError(t, os.WriteFile(filepath.Join(root, "config.json"), []byte(main), 0600))
			cfg, err := (config.Loader{}).Load(context.Background(), filepath.Join(root, "config.json"))
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
				return
			}
			require.NoError(t, err)
			if tc.name == "disabled legacy" {
				require.False(t, cfg.Caches["shared"].Enabled)
			}
		})
	}
}

func TestCacheConfigConstantsAndIsolation(t *testing.T) {
	for _, withConstants := range []bool{false, true} {
		cfg := &config.Config{Caches: map[string]*spec.CacheSettings{"shared": {
			Enabled: true, Provider: "${Provider}", Location: "${CacheRoot}/${View.Name}", TTL: "1m",
			Warmup: &spec.CacheWarmupSettings{FieldNames: []string{"id"}},
		}}}
		if withConstants {
			var err error
			cfg.Const, err = constant.New(map[string]string{"Provider": "afs", "CacheRoot": t.TempDir()})
			require.NoError(t, err)
		}
		before, err := json.Marshal(cfg)
		require.NoError(t, err)
		resolved, err := cfg.ResolveConstants()
		require.NoError(t, err)
		if withConstants {
			require.Equal(t, "afs", resolved.Caches["shared"].Provider)
		}
		resolved.Caches["shared"].Location = "changed"
		resolved.Caches["shared"].Warmup.FieldNames[0] = "changed"
		after, err := json.Marshal(cfg)
		require.NoError(t, err)
		require.JSONEq(t, string(before), string(after))
	}
}

func TestLoaderCaseVariantCacheKeys(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		for _, dependency := range []bool{false, true} {
			for _, body := range []string{
				`{"Caches":{"shared":{"Enabled":true,"Location":"first","TTL":"1m"}},"caches":{"shared":{"Enabled":true,"Location":"second","TTL":"2m"}}}`,
				`{"CacheProviders":[{"Name":"shared","Location":"first","TTL":"1m"}],"cacheproviders":[{"Name":"shared","Location":"second","TTL":"2m"}]}`,
				`{"Caches":{"shared":{"Enabled":false,"enabled":true,"Location":"cache","TTL":"1m"}}}`,
				`{"CacheProviders":[{"Name":"first","name":"second","Location":"cache","TTL":"1m"}]}`,
			} {
				root := t.TempDir()
				data := []byte(body)
				if format == "yaml" {
					var document map[string]any
					require.NoError(t, json.Unmarshal(data, &document))
					var err error
					data, err = yaml.Marshal(document)
					require.NoError(t, err)
				}
				location := filepath.Join(root, "cache."+format)
				require.NoError(t, os.WriteFile(location, data, 0600))
				if dependency {
					location = filepath.Join(root, "config.json")
					require.NoError(t, os.WriteFile(location, []byte(`{"DependencyURL":"cache.`+format+`"}`), 0600))
				}
				_, err := (config.Loader{}).Load(context.Background(), location)
				require.ErrorContains(t, err, "duplicate JSON key", "format=%s dependency=%t body=%s", format, dependency, body)
			}
		}
	}
	// Application-defined map keys are not case-insensitive struct field aliases.
	location := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(location, []byte(`{"Caches":{"Shared":{"Enabled":true,"Location":"one","TTL":"1m"},"shared":{"enabled":true,"location":"two","ttl":"2m"}}}`), 0600))
	cfg, err := (config.Loader{}).Load(context.Background(), location)
	require.NoError(t, err)
	require.Len(t, cfg.Caches, 2)
	require.Equal(t, "one", cfg.Caches["Shared"].Location)
	require.Equal(t, "two", cfg.Caches["shared"].Location)
}
