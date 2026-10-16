package standalone

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestIndexedReaderRetainsDatabaseNullabilityForScalarOutput(t *testing.T) {
	f := fixture.New(t)
	if err := f.DB.ExecStatements(context.Background(), "UPDATE records SET name=NULL WHERE id=1"); err != nil {
		t.Fatal(err)
	}
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
	res := httptest.NewRecorder()
	server.manager.ServeHTTP(res, httptest.NewRequest("GET", "/records/1", nil))
	var result struct {
		Rows []records.Record `json:"rows"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &result); err != nil || res.Code != 200 || len(result.Rows) != 1 || result.Rows[0].ID != 1 || result.Rows[0].Name != "" {
		t.Fatalf("nullable SQL column into original scalar output: %d %s (%v)", res.Code, res.Body.String(), err)
	}
}

func TestLinkedReaderRetainsDatabaseNullabilityForScalarOutput(t *testing.T) {
	f := fixture.New(t)
	if err := f.DB.ExecStatements(context.Background(), "UPDATE records SET name=NULL WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	cfg.GoBootstrap.LinkedOnly = true
	cfg.BaseDir = filepath.Join(t.TempDir(), "missing-source")
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
	res := httptest.NewRecorder()
	server.manager.ServeHTTP(res, httptest.NewRequest("GET", "/records/1", nil))
	var result struct {
		Rows []records.Record `json:"rows"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &result); err != nil || res.Code != 200 || len(result.Rows) != 1 || result.Rows[0].ID != 1 || result.Rows[0].Name != "" {
		t.Fatalf("nullable SQL column into original scalar output: %d %s (%v)", res.Code, res.Body.String(), err)
	}
}

func TestLinkedReaderBootstrapDoesNotRequireDatabaseSchema(t *testing.T) {
	ctx := context.Background()
	f := fixture.New(t)
	if err := f.DB.ExecStatements(ctx, "DROP TABLE records"); err != nil {
		t.Fatal(err)
	}
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	if err != nil {
		t.Fatal(err)
	}
	cfg.GoBootstrap.LinkedOnly = true
	cfg.BaseDir = filepath.Join(t.TempDir(), "missing-source")
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(ctx, Options{Config: cfg, Registry: exports})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	build, err := server.source.compileLinked(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	materialized := 0
	for _, entry := range build.Index.Entries() {
		if entry.Owner.Name != "" {
			continue
		}
		if _, err := build.Materializer.Materialize(ctx, entry, nil); err != nil {
			t.Fatal(err)
		}
		materialized++
	}
	if materialized == 0 {
		t.Fatal("no linked component was materialized")
	}
	// Generated shape and embedded SQL must bootstrap without authoring-time
	// queries against a table that may not exist until deployment provisioning.
	if err := server.Reload(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := f.DB.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO records VALUES(1,NULL)"); err != nil {
		t.Fatal(err)
	}
	res := httptest.NewRecorder()
	server.manager.ServeHTTP(res, httptest.NewRequest("GET", "/records/1", nil))
	if res.Code != 200 {
		t.Fatalf("generated linked read after provisioning: %d %s", res.Code, res.Body.String())
	}
}
