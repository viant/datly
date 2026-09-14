package app

import (
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"testing"
)

// Services authors the real cache-bearing DQL overlay and standalone policies.
func (f *Fixture) Services(t *testing.T, collector string) {
	t.Helper()
	query := fmt.Sprintf(`#setting($_ = $route('/records/{id}', 'GET'))
#setting($_ = $input_type('ReadInput'))
#setting($_ = $output_type('ReadOutput'))
#setting($_ = $cache(true, '1m').WithLocation('%s'))
#setting($_ = $cache_warmup('', 'id=1'))
SELECT records.* FROM (SELECT id,name FROM records WHERE id=:ID) records`, filepath.ToSlash(filepath.Join(f.Root, "cache")))
	if err := os.WriteFile(filepath.Join(f.Root, "records/Read.dql"), []byte(query), 0600); err != nil {
		t.Fatal(err)
	}
	f.WriteConfig(t, func(c map[string]any) {
		c["CORS"] = map[string]any{"AllowOrigins": []string{"https://client.example"}, "AllowMethods": []string{"GET", "HEAD", "POST"}, "AllowHeaders": []string{"X-Read", "X-Admin", "X-Document", "Content-Type"}, "ExposeHeaders": []string{"Content-Type"}, "AllowCredentials": false, "MaxAge": 600}
		c["APIKeys"] = []any{map[string]any{"URI": "/records", "Header": "X-Component", "Value": "component-key"}, map[string]any{"URI": "/records/", "Header": "X-Read", "Value": "read-key"}}
		c["Meta"] = map[string]any{"OpenApiURI": "/schema", "DocURI": "/docs", "CacheWarmURI": "/warm"}
		c["Warmup"] = map[string]any{"TimeoutMs": 2000, "Admin": map[string]any{"APIKeyHeader": "X-Admin", "APIKeyValue": "admin-key"}}
		c["OpenAPI"] = map[string]any{"Info": map[string]any{"title": "Configured services", "version": "1"}, "AggregateAccess": map[string]any{"APIKeyHeader": "X-Document", "APIKeyValue": "document-key"}, "RouteAccess": map[string]any{"APIKeyHeader": "X-Document", "APIKeyValue": "route-document-key"}, "StartupExports": []any{map[string]any{"URL": "schema.json", "Format": "json"}, map[string]any{"URL": "record.yaml", "Format": "yaml", "Path": "/records/{id}"}}}
		c["Observation"] = map[string]any{"LogSummaries": true}
		if collector != "" {
			c["Observation"].(map[string]any)["OTel"] = map[string]any{"Enabled": true, "QueueSize": 16, "BatchSize": 1, "MaxSpans": 128, "BatchTimeoutMs": 1, "ExportTimeoutMs": 250, "ServiceName": "configured-app", "ServiceVersion": "services-test", "HTTP": map[string]any{"EndpointURL": collector + "/v1/traces", "Insecure": true, "Headers": map[string]string{"Authorization": "collector-key"}}}
		}
	})
}

func (f *Fixture) YAML(t *testing.T) {
	t.Helper()
	data, err := os.ReadFile(f.Config)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err = json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	data, err = yaml.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	f.Config = filepath.Join(f.Root, "config.yaml")
	if err = os.WriteFile(f.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
}
