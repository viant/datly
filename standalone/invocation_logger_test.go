package standalone

import (
	"context"
	"encoding/json"
	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/loggerapp"
	"github.com/viant/datly/standalone/testdata/loggerapp/components"
	h "github.com/viant/xdatly/handler"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func loggerServer(t *testing.T, log *fixture.Logger) *Server {
	t.Helper()
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	options := Options{Config: cfg, Holders: []any{components.Component{}}, RequireLinked: true}
	if log != nil {
		options.InvocationLogger = log
	}
	server, err := New(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })
	if err = server.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	return server
}
func TestHostInvocationLoggerBindsReaderOutputChildAndWriterLifecycle(t *testing.T) {
	log := &fixture.Logger{}
	server := loggerServer(t, log)
	send := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		server.manager.ServeHTTP(rec, req)
		return rec
	}
	child := send("GET", "/parent/1", "")
	if child.Code != 200 {
		t.Fatalf("child %d %s", child.Code, child.Body.String())
	}
	var out components.ParentOutput
	if err := json.Unmarshal(child.Body.Bytes(), &out); err != nil || len(out.Rows) != 1 || out.Rows[0].Name != "first" {
		t.Fatalf("child result %#v %v", out, err)
	}
	if strings.Contains(child.Body.String(), "Logger") || strings.Contains(child.Body.String(), "logger") {
		t.Fatal("host capability leaked into body")
	}
	expected := []fixture.Entry{{Level: "info", Message: "parent.begin", Attributes: []any{"id", 1}}, {Level: "debug", Message: "reader.input", Attributes: []any{"id", 1}}, {Level: "info", Message: "parent.end", Attributes: []any{"rows", 1}}, {Level: "warn", Message: "reader.output", Attributes: []any{"rows", 1}}}
	if actual := log.Snapshot(); !reflect.DeepEqual(actual, expected) {
		t.Fatalf("configured logger/order %#v", actual)
	}
	created := send("POST", "/logged", `{"data":[{"id":2,"name":"created"}]}`)
	if created.Code != 200 {
		t.Fatalf("writer %d %s", created.Code, created.Body.String())
	}
	entries := log.Snapshot()
	if len(entries) != 6 || entries[4].Message != "writer.init" || entries[5].Message != "writer.finalize" || entries[4].Level != "info" || entries[5].Level != "info" {
		t.Fatalf("lifecycle host logger %#v", entries)
	}
	var name string
	if err := server.source.connections.SQL.DB.QueryRow("SELECT name FROM records WHERE id=2").Scan(&name); err != nil || name != "created" {
		t.Fatalf("native writer persistence %q %v", name, err)
	}
}
func TestNilHostLoggerRetainsMissingCapabilityFailure(t *testing.T) {
	server := loggerServer(t, nil)
	for _, path := range []string{"/logged/1", "/parent/1"} {
		rec := httptest.NewRecorder()
		server.manager.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		if rec.Code != 500 {
			t.Fatalf("missing logger %s %d %s", path, rec.Code, rec.Body.String())
		}
	}
	req := httptest.NewRequest("POST", "/logged", strings.NewReader(`{"data":[{"id":2,"name":"must-not-write"}]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.manager.ServeHTTP(rec, req)
	if rec.Code != 500 {
		t.Fatalf("missing lifecycle logger %d %s", rec.Code, rec.Body.String())
	}
	var count int
	if err := server.source.connections.SQL.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 1 {
		t.Fatalf("missing logger persisted writer rows %d %v", count, err)
	}

}
func TestHostLoggerDoesNotAuthorizeSpoofedApplicationProvider(t *testing.T) {
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(context.Background(), Options{Config: cfg, Holders: []any{components.Component{}}, InvocationLogger: &fixture.Logger{}, Providers: []locator.Provider{provider.Static(h.ValueKey("logger"), &fixture.Logger{})}})
	if err == nil {
		defer server.Shutdown(context.Background())
		err = server.Reload(context.Background(), 1)
		if err == nil {
			_, err = server.manager.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: components.Package, Name: "Read"}, Route: spec.RouteRef{Method: "GET", Path: "/logged/{id}"}}, Input: &components.ReadInput{ID: 1}})
		}
	}
	if err == nil {
		t.Fatal("spoofed provider was accepted")
	}

	if !strings.Contains(err.Error(), "protected") && !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("unexpected spoofing rejection %v", err)
	}
}
