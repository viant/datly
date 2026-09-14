package command_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/cmd/command"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/otlp"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestExecutableConfiguredHTTPAndOTLP(t *testing.T) {
	collector := otlp.New(t, nil)
	f := fixture.New(t)
	f.Services(t, collector.Server.URL)
	f.YAML(t)
	binary := filepath.Join(t.TempDir(), "app")
	build := exec.Command("go", "build", "-o", binary, "../../standalone/testdata/app/cmd")
	build.Env = append(os.Environ(), "GOWORK=off")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %v %s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var stdout, stderr testharness.Output
	process := exec.Command(binary, "run", "-conf", f.Config)
	process.Stdout = &stdout
	process.Stderr = &stderr
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait(); cancel() }()
	t.Cleanup(func() {
		_ = process.Process.Kill()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("CLI did not exit")
		}
	})
	address, err := stdout.WaitLine(ctx, "HTTP listening on ")
	if err != nil {
		t.Fatalf("ready %v %s", err, stderr.String())
	}
	client := &http.Client{Timeout: 3 * time.Second}
	for _, tc := range []struct {
		method, path, body string
		headers            map[string]string
		status             int
	}{
		{"OPTIONS", "/records/1", "", nil, 204},
		{"OPTIONS", "/records/1", "", map[string]string{"Origin": "https://client.example", "Access-Control-Request-Method": "GET", "Access-Control-Request-Headers": "X-Read"}, 204},
		{"GET", "/records/1", "", map[string]string{"X-Read": "read-key"}, 200},
		{"POST", "/warm/records/1", "", map[string]string{"X-Read": "read-key"}, 403},
		{"POST", "/warm/records/1", "", map[string]string{"X-Read": "read-key", "X-Admin": "admin-key"}, 200},
		{"GET", "/docs", "", map[string]string{"X-Document": "document-key"}, 200},
		{"GET", "/schema/records/1", "", map[string]string{"X-Document": "route-document-key"}, 200},
		{"POST", "/records", `{"data":{"id":2,"name":"private-body"}}`, map[string]string{"X-Component": "component-key", "Content-Type": "application/json"}, 200},
	} {
		request, _ := http.NewRequestWithContext(ctx, tc.method, "http://"+address+tc.path, strings.NewReader(tc.body))
		for k, v := range tc.headers {
			request.Header.Set(k, v)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != tc.status {
			t.Fatalf("%s %s: %d %s", tc.method, tc.path, response.StatusCode, data)
		}
	}
	if _, err := os.Stat(filepath.Join(f.Root, "schema.json")); err != nil {
		t.Fatal(err)
	}
	if err := collector.Wait(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if err := process.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown %v %s", err, stderr.String())
		}
		done <- err
	case <-ctx.Done():
		t.Fatal("shutdown timed out")
	}
	requests, keys := collector.Snapshot()
	serviceFound, dmlFound := false, false
	for i, r := range requests {
		if keys[i] != "collector-key" {
			t.Fatal("exporter credential not applied")
		}
		encoded, _ := protojson.Marshal(r)
		for _, secret := range []string{"private-body", "read-key", "admin-key", "collector-key", "SELECT ", "INSERT INTO"} {
			if bytes.Contains(encoded, []byte(secret)) {
				t.Fatalf("private data in export: %s", secret)
			}
		}
		for _, resource := range r.ResourceSpans {
			for _, attribute := range resource.Resource.Attributes {
				if attribute.Key == "service.name" && attribute.Value.GetStringValue() == "configured-app" {
					serviceFound = true
				}
			}
			for _, scope := range resource.ScopeSpans {
				for _, span := range scope.Spans {
					for _, attribute := range span.Attributes {
						if attribute.Key == "db.operation.name" && attribute.Value.GetStringValue() == "INSERT" {
							dmlFound = true
						}
					}
				}
			}
		}
	}
	if !serviceFound {
		t.Fatal("service configuration not exported")
	}
	if !dmlFound {
		t.Fatal("configured exporter did not receive native INSERT capture")
	}
	if strings.Contains(stdout.String(), "SELECT ") || strings.Contains(stderr.String(), "private-body") || strings.Contains(stderr.String(), "admin-key") {
		t.Fatal("raw SQL/data/credentials leaked to diagnostics")
	}
}

func TestServicesCLIRejectsInvalidConfiguration(t *testing.T) {
	for _, name := range []string{"warmup", "exporter", "documentation"} {
		t.Run(name, func(t *testing.T) {
			f := fixture.New(t)
			f.Services(t, "")
			data, err := os.ReadFile(f.Config)
			if err != nil {
				t.Fatal(err)
			}
			var cfg map[string]any
			if err = json.Unmarshal(data, &cfg); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "warmup":
				cfg["Warmup"].(map[string]any)["TimeoutMs"] = 0
			case "exporter":
				cfg["Observation"] = map[string]any{"OTel": map[string]any{"Enabled": true, "HTTP": map[string]any{"EndpointURL": "https://user:private-credential@example.com/v1/traces"}}}
			case "documentation":
				cfg["OpenAPI"].(map[string]any)["StartupExports"] = []any{map[string]any{"URL": "bad", "Format": "unsupported"}}
			}
			data, err = json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(f.Config, data, 0600); err != nil {
				t.Fatal(err)
			}
			exports, err := records.Exports()
			if err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			if status := (command.Service{Registry: exports}).Run(context.Background(), []string{"run", "-conf", f.Config}, &stdout, &stderr); status != 1 || stdout.Len() != 0 || strings.Contains(stderr.String(), "private-credential") {
				t.Fatalf("status=%d out=%s diagnostics=%s", status, &stdout, &stderr)
			}
		})
	}
}
