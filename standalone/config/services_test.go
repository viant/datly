package config_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/datly/standalone/config"
)

func TestServicesInvalidConfiguration(t *testing.T) {
	for _, extra := range []string{
		`"Warmup":{"TimeoutMs":10}`,
		`"Warmup":{"TimeoutMs":0,"Admin":{"APIKeyHeader":"X-Admin","APIKeyValue":"key"}}`,
		`"Warmup":{"TimeoutMs":9223372036854775807,"Admin":{"APIKeyHeader":"X-Admin","APIKeyValue":"key"}}`,
		`"Observation":{"OTel":{"Enabled":true,"HTTP":{"EndpointURL":"http://localhost/v1/traces"}}}`,
		`"Observation":{"OTel":{"Enabled":true,"QueueSize":-1}}`,
		`"Observation":{"OTel":{"Enabled":true,"QueueSize":9223372036854775807}}`,
		`"Observation":{"OTel":{"Enabled":true,"HTTP":{"EndpointURL":"https://user:password@localhost/v1/traces"}}}`,
		`"OpenAPI":{"StartupExports":[{"URL":"https://example.com/file","Format":"json"}]}`,
		`"OpenAPI":{"StartupExports":[{"URL":"schema.txt","Format":"text"}]}`,
	} {
		t.Run(extra, func(t *testing.T) {
			var c config.Config
			if err := json.Unmarshal([]byte(`{"GoBootstrap":{"Packages":["example.com/app"]},`+extra+`}`), &c); err != nil {
				t.Fatal(err)
			}
			if err := c.Validate(); err == nil {
				t.Fatal("invalid service configuration accepted")
			}
		})
	}
}

func TestServicesDisabledAndRelativeExports(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "services.yaml")
	content := "GoBootstrap:\n  Packages: [example.com/app]\nObservation:\n  OTel:\n    Enabled: false\nOpenAPI:\n  StartupExports:\n    - URL: schema.json\n      Format: json\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := (config.Loader{}).Load(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.Observation.OTel.Enabled || c.OpenAPI.StartupExports[0].URL != filepath.Join(dir, "schema.json") {
		t.Fatal("disabled or relative options lost")
	}
}
