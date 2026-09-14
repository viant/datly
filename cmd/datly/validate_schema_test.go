package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe"
)

func TestSchemaDSNPreservesSelectedFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	for _, name := range []string{"schema#prod.db", "schema", "100%.db", "schema%23.db", "schema#.db", "plain.db"} {
		h := sqlite.New(t, sqlite.WithDSN((&url.URL{Scheme: "file", Path: filepath.Join(dir, name)}).String()))
		if err := h.ExecStatements(ctx, "CREATE TABLE proof(marker TEXT)"); err != nil {
			t.Fatal(err)
		}
		if _, err := h.DB.ExecContext(ctx, "INSERT INTO proof VALUES(?)", name); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"schema#prod.db", "100%.db", "schema%23.db", "plain.db"} {
		path := filepath.Join(dir, name)
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		relative, err := filepath.Rel(cwd, path)
		if err != nil {
			t.Fatal(err)
		}
		relativeURI := "file:" + (&url.URL{Path: filepath.ToSlash(relative)}).EscapedPath()
		for _, dsn := range []string{path, path + "?mode=rw&_query_only=false", (&url.URL{Scheme: "file", Path: path}).String(), relativeURI} {
			o := &schemaOptions{connector: "main", driver: "sqlite3", dsn: dsn}
			db, err := o.ResolveDB(ctx, "main")
			if err != nil {
				t.Fatal(err)
			}
			var value string
			err = db.QueryRowContext(ctx, "SELECT marker FROM proof").Scan(&value)
			if err != nil || value != name {
				t.Fatalf("DSN %q selected %q: %v", dsn, value, err)
			}
			if _, err = db.ExecContext(ctx, "INSERT INTO proof VALUES('wrong')"); err == nil {
				t.Fatal("read-only override lost")
			}
			o.close()
		}
	}
	for _, dsn := range []string{"file:", "file:?cache=shared", "file://localhost", ":memory:", "file::memory:", "file:%3Amemory%3A", "file:plain.db#fragment"} {
		o := &schemaOptions{connector: "main", driver: "sqlite3", dsn: dsn}
		if _, err := o.ResolveDB(ctx, "main"); err == nil {
			o.close()
			t.Errorf("non-file or ambiguous DSN accepted: %q", dsn)
		}
	}
}

func TestValidateSchemaRequiresOptIn(t *testing.T) {
	for _, options := range [][]string{{"-schema"}, {"-dsn", "unused.db"}, {"-connector", "main"}, {"-schema", "-driver", "sqlite3", "-dsn", "unused.db"}} {
		var output, diagnostic bytes.Buffer
		args := append([]string{"validate"}, options...)
		args = append(args, "example.com/app")
		if status := run(context.Background(), args, &output, &diagnostic); status != 2 || diagnostic.Len() == 0 {
			t.Fatalf("args=%v status=%d output=%s diagnostic=%s", args, status, &output, &diagnostic)
		}
	}
}

func TestValidateSchemaCLIReadOnlySQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER NOT NULL)", "INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(h.TempDir, "test.db")
	for _, tc := range []struct {
		name, query, dsn, driver string
		status                   int
	}{
		{"valid", "SELECT r.id FROM records r", database, "sqlite3", 0},
		{"missing column", "SELECT r.absent FROM records r", database, "sqlite3", 1},
		{"missing table", "SELECT r.id FROM absent r", database, "sqlite3", 1},
		{"missing DB", "SELECT r.id FROM records r", filepath.Join(h.TempDir, "absent.db"), "sqlite3", 1},
		{"bad driver", "SELECT r.id FROM records r", database, "not-a-driver", 1},
		{"memory DB", "SELECT r.id FROM records r", "file::memory:?cache=shared", "sqlite3", 1},
		{"connector override", "#setting($_ = $connector('other'))\nSELECT r.id FROM records r", database, "sqlite3", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			if err := os.MkdirAll(filepath.Join(base, "records"), 0755); err != nil {
				t.Fatal(err)
			}
			for file, content := range map[string]string{"go.mod": "module example.com/app\n\ngo 1.25.0\n", "records/Records.dql": "#setting($_ = $route('/records','GET'))\n" + tc.query} {
				if err := os.WriteFile(filepath.Join(base, file), []byte(content), 0644); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.ReadFile(database)
			if err != nil {
				t.Fatal(err)
			}
			var output, diagnostics bytes.Buffer
			status := run(ctx, []string{"validate", "-schema", "-connector", "main", "-driver", tc.driver, "-dsn", tc.dsn, "-dir", base, "-format", "json", "example.com/app/records"}, &output, &diagnostics)
			var report transcribe.ValidationReport
			if err := json.Unmarshal(output.Bytes(), &report); err != nil || status != tc.status || report.Valid != (tc.status == 0) {
				t.Fatalf("status=%d report=%s diagnostic=%s error=%v", status, &output, &diagnostics, err)
			}
			if status != 0 && len(report.Diagnostics) == 0 {
				t.Fatal("missing diagnostic")
			}
			if status == 0 && len(report.Schema) != 1 {
				t.Fatalf("missing inspection: %+v", report)
			}
			if status == 0 {
				output.Reset()
				status = run(ctx, []string{"validate", "-schema", "-connector", "main", "-driver", tc.driver, "-dsn", tc.dsn, "-dir", base, "example.com/app/records"}, &output, &diagnostics)
				if status != 0 || !strings.Contains(output.String(), "Schema-aware authoring validation passed: true") || !strings.Contains(output.String(), "Inspected:") {
					t.Fatalf("text status=%d output=%s", status, &output)
				}
			}
			after, err := os.ReadFile(database)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("database modified")
			}
			if _, err := os.Stat(filepath.Join(base, "generated")); !os.IsNotExist(err) {
				t.Fatalf("generated output written: %v", err)
			}
			if _, err := os.Stat(filepath.Join(h.TempDir, "absent.db")); !os.IsNotExist(err) {
				t.Fatalf("missing database was created: %v", err)
			}
		})
	}
	options := &schemaOptions{connector: "main", driver: "sqlite3", dsn: "file:" + database + "?mode=rwc&_query_only=false"}
	defer options.close()
	db, err := options.ResolveDB(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO records VALUES(2)"); err == nil || !strings.Contains(strings.ToLower(err.Error()), "readonly") {
		t.Fatalf("discovery connection is writable: %v", err)
	}
}
