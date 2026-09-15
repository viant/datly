package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestInstanceFileCLISchemaAndGeneration(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE `e2e.ds.records` (id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	dir := filepath.Join(root, "source")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	text := "#package('api/records')\n#setting($_ = $route('/records','GET'))\n#setting($_ = $const('project','authored'))\nSELECT id,name FROM `$project.ds.records`"
	path := filepath.Join(dir, "Records.dql")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(map[string]string{"project": "e2e", "Database": filepath.Join(h.TempDir, "test.db")})
	file := filepath.Join(root, "instance.json")
	if err := os.WriteFile(file, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	for _, operation := range [][]string{{"validate"}, {"transcribe", "get"}} {
		args := append(append([]string(nil), operation...), "-dir", root, "-const", filepath.Join(root, "not-loaded.yaml"), "-const", file, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", "${Database}", "example.com/generated/source")
		var out, diagnostic bytes.Buffer
		if code := run(ctx, args, &out, &diagnostic); code != 0 {
			t.Fatalf("%v: %d %s %s", operation, code, &out, &diagnostic)
		}
	}
	after, _ := os.ReadFile(path)
	if string(after) != text {
		t.Fatal("CLI changed DQL source")
	}
	err := filepath.WalkDir(filepath.Join(root, "api"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".sql" {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(content), "e2e.ds.records") {
			t.Errorf("CLI persisted expanded SQL at %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
