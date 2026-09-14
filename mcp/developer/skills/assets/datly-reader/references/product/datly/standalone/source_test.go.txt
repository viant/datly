package standalone

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/application"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestSourceManagerReadMutationReloadSQLite(t *testing.T) {
	f := fixture.New(t)
	f.WriteConfig(t, func(c map[string]any) {
		c["Info"] = map[string]any{"title": "Records API", "version": "1"}
		c["CORS"] = map[string]any{"AllowOrigins": []string{"https://client.example"}}
	})
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(context.Background(), Options{Config: cfg, Registry: exports})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := server.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	read := func(want string) {
		t.Helper()
		res := httptest.NewRecorder()
		server.manager.ServeHTTP(res, httptest.NewRequest("GET", "/records/1", nil))
		var result struct {
			Rows []records.Record `json:"rows"`
		}
		err := json.Unmarshal(res.Body.Bytes(), &result)
		if err != nil || res.Code != 200 || len(result.Rows) != 1 || result.Rows[0].Name != want {
			t.Fatalf("read %d %s %v", res.Code, res.Body.String(), err)
		}
	}
	read("first")
	policyResponse := httptest.NewRecorder()
	policyRequest := httptest.NewRequest("GET", "/records/1", nil)
	policyRequest.Header.Set("Origin", "https://client.example")
	server.manager.ServeHTTP(policyResponse, policyRequest)
	if policyResponse.Header().Get("Access-Control-Allow-Origin") != "https://client.example" {
		t.Fatal("configured CORS was not applied")
	}
	policyResponse = httptest.NewRecorder()
	server.manager.ServeHTTP(policyResponse, httptest.NewRequest("GET", "/v1/api/meta/openapi", nil))
	if policyResponse.Code != 200 || !strings.Contains(policyResponse.Body.String(), "Records API") {
		t.Fatalf("OpenAPI %d %s", policyResponse.Code, policyResponse.Body.String())
	}
	res := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/records", strings.NewReader(`{"data":{"id":2,"name":"created"}}`))
	req.Header.Set("Content-Type", "application/json")
	server.manager.ServeHTTP(res, req)
	if res.Code != 200 || !strings.Contains(res.Body.String(), `"finalized":true`) {
		t.Fatalf("mutation %d %s", res.Code, res.Body.String())
	}
	f.DB.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT id,name FROM records ORDER BY id"}, []records.Record{{ID: 1, Name: "first"}, {ID: 2, Name: "created"}})
	query := filepath.Join(f.Root, "records/queries/read.sql")
	if err := os.WriteFile(query, []byte("SELECT id, 'reloaded' AS name FROM records WHERE id=:ID"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := server.Reload(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	read("reloaded")
	if err := os.Remove(query); err != nil {
		t.Fatal(err)
	}
	if err := server.Reload(context.Background(), 3); err == nil {
		t.Fatal("missing resource published")
	}
	if server.manager.Revision() != 2 {
		t.Fatal("failed reload replaced generation")
	}
	read("reloaded")
	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.Reload(context.Background(), 4); !errors.Is(err, application.ErrClosed) {
		t.Fatalf("reload after shutdown %v", err)
	}
	if err := server.source.connections.SQL.DB.Ping(); err == nil {
		t.Fatal("owned database stayed open")
	}
	if err := f.DB.DB.Ping(); err != nil {
		t.Fatalf("independent caller database was closed: %v", err)
	}
}

func TestSourceDQLOverlayAndFailures(t *testing.T) {
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(context.Background(), Options{Config: cfg, Registry: exports})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())
	overlay := filepath.Join(f.Root, "records/Read.dql")
	if err := os.WriteFile(overlay, []byte("#setting($_ = $route('/alias/{id}', 'GET'))\n#setting($_ = $input_type('ReadInput'))\n#setting($_ = $output_type('ReadOutput'))\nSELECT records.* FROM (SELECT id,name FROM records WHERE id=:ID) records"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = server.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	res := httptest.NewRecorder()
	server.manager.ServeHTTP(res, httptest.NewRequest("GET", "/alias/1", nil))
	if res.Code != 200 {
		t.Fatalf("overlay %d %s", res.Code, res.Body.String())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = server.Reload(ctx, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reload: %v", err)
	}
	if err = os.WriteFile(overlay, []byte("#setting($_ = $route('/alias/{id}', 'GET'))\n#set($bad = )"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = server.Reload(context.Background(), 2); err == nil {
		t.Fatal("invalid DQL published")
	}
	if server.manager.Revision() != 1 {
		t.Fatal("failed stage changed revision")
	}
}

func TestShutdownDeadlineRetainsConnectionsUntilRequestsFinish(t *testing.T) {
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(context.Background(), Options{Config: cfg, Registry: exports})
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	tracked := server.track(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		server.manager.ServeHTTP(w, r)
	}))
	res := httptest.NewRecorder()
	go func() { defer close(finished); tracked.ServeHTTP(res, httptest.NewRequest("GET", "/records/1", nil)) }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err = server.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("shutdown %v", err)
	}
	if err = server.source.connections.SQL.DB.Ping(); err != nil {
		t.Errorf("database closed before request completed: %v", err)
	}
	if err = server.Reload(context.Background(), 2); !errors.Is(err, application.ErrClosed) {
		t.Errorf("reload during drain %v", err)
	}
	close(release)
	<-finished
	if res.Code != 200 {
		t.Errorf("admitted request did not drain: %d %s", res.Code, res.Body.String())
	}
	if err = server.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = server.source.connections.SQL.DB.Ping(); err == nil {
		t.Fatal("database remained open")
	}
}
