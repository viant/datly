package developer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"github.com/viant/datly/transcribe"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestSchemaNamedConnectionsRemainDistinctAndReadOnly(t *testing.T) {
	ctx := context.Background()
	entries := []map[string]string{}
	for _, name := range []string{"main", "history"} {
		h := sqlite.New(t)
		if err := h.ExecStatements(ctx, "CREATE TABLE proof(marker TEXT)"); err != nil {
			t.Fatal(err)
		}
		if _, err := h.DB.ExecContext(ctx, "INSERT INTO proof VALUES(?)", name); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, map[string]string{"name": name, "driver": "sqlite3", "dsn": filepath.Join(h.TempDir, "test.db") + "?mode=rw&_query_only=false"})
	}
	data, _ := json.Marshal(entries)
	path := filepath.Join(t.TempDir(), "connectors.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	options := &schemaOptions{enabled: true, connector: "main", connectorsFile: path}
	if err := options.validate(); err != nil {
		t.Fatal(err)
	}
	defer options.close()
	dbs := map[string]*sql.DB{}
	for _, entry := range entries {
		db, err := options.ResolveDB(ctx, entry["name"])
		if err != nil {
			t.Fatal(err)
		}
		var marker string
		if err = db.QueryRowContext(ctx, "SELECT marker FROM proof").Scan(&marker); err != nil || marker != entry["name"] {
			t.Fatalf("selected %q: %v", marker, err)
		}
		if _, err = db.ExecContext(ctx, "INSERT INTO proof VALUES('wrong')"); err == nil {
			t.Fatal("discovery DB is writable")
		}
		dbs[entry["name"]] = db
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name, want := range dbs {
				got, err := options.ResolveDB(ctx, name)
				if err != nil || got != want {
					t.Errorf("reopened %s: %v", name, err)
				}
			}
		}()
	}
	wg.Wait()
	if _, err := options.ResolveDB(ctx, "unknown"); err == nil {
		t.Fatal("unknown connector accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := options.ResolveDB(canceled, "history"); err == nil {
		t.Fatal("canceled discovery accepted")
	}
	options.close()
	for name, db := range dbs {
		if err := db.PingContext(ctx); err == nil {
			t.Errorf("%s remained open", name)
		}
	}
}

func TestSchemaNamedConnectionConfigurationRejectsAmbiguity(t *testing.T) {
	for _, data := range []string{`null`, `[]`, `[{"name":"other","driver":"sqlite3","dsn":"proof.db"}]`, `[{"name":"main","driver":"sqlite3","dsn":"proof.db"},{"name":" main ","driver":"sqlite3","dsn":"other.db"}]`, `[{"name":"main","driver":"sqlite3"}]`, `[{"name":"main","driver":"sqlite3","dsn":"proof.db","ignored":true}]`, `[{"name":"main","driver":"sqlite3","dsn":"proof.db"}] []`} {
		path := filepath.Join(t.TempDir(), "connectors.json")
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if err := (&schemaOptions{enabled: true, connector: "main", connectorsFile: path}).validate(); err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
	if err := (&schemaOptions{connector: "main", connectorsFile: "ignored.json"}).validate(); err == nil {
		t.Fatal("opt-in not required")
	}
	if err := (&schemaOptions{enabled: true, connector: "main", driver: "sqlite3", dsn: "proof.db", connectorsFile: "ignored.json"}).validate(); err == nil {
		t.Fatal("ambiguous config accepted")
	}
}

func TestValidateSchemaCLIMultipleNamedConnections(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	entries := []map[string]string{}
	if err := os.WriteFile(filepath.Join(base, "go.mod"), []byte("module example.com/app\n\ngo 1.25.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"main", "history"} {
		h := sqlite.New(t)
		if err := h.ExecStatements(ctx, "CREATE TABLE "+name+"_records(id INTEGER NOT NULL)"); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, map[string]string{"name": name, "driver": "sqlite3", "dsn": filepath.Join(h.TempDir, "test.db")})
		dir := filepath.Join(base, name)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		source := "#setting($_ = $route('/" + name + "','GET'))\n#setting($_ = $connector('" + name + "'))\nSELECT r.id FROM " + name + "_records r"
		if err := os.WriteFile(filepath.Join(dir, "Records.dql"), []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
	}
	data, _ := json.Marshal(entries)
	config := filepath.Join(base, "connectors.json")
	if err := os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	var output, diagnostics bytes.Buffer
	status := Run(ctx, []string{"validate", "-schema", "-connector", "main", "-schema-connectors", config, "-dir", base, "-format", "json", "example.com/app/main", "example.com/app/history"}, &output, &diagnostics)
	var report transcribe.ValidationReport
	if err := json.Unmarshal(output.Bytes(), &report); err != nil || status != 0 || !report.Valid || len(report.Schema) != 2 {
		t.Fatalf("status=%d output=%s diagnostic=%s err=%v", status, &output, &diagnostics, err)
	}
}
