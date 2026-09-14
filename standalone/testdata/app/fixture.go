package app

import (
	"context"
	"embed"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
)

//go:embed records authrecords asyncrecords spend
var Files embed.FS

const Module = "github.com/viant/datly/standalone/testdata/app"

type Fixture struct {
	Root, Config, DSN string
	DB                *sqlite.Harness
}

func New(t *testing.T) *Fixture {
	t.Helper()
	f := &Fixture{Root: t.TempDir()}
	f.Config = filepath.Join(f.Root, "config.json")
	f.DSN = filepath.Join(f.Root, "records.db")
	f.DB = sqlite.New(t, sqlite.WithDSN(f.DSN))
	if err := f.DB.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO records VALUES(1,'first')"); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(f.Root, Files); err != nil {
		t.Fatal(err)
	}
	(testharness.GeneratedModule{Path: Module}).Write(t, f.Root)
	f.WriteConfig(t, nil)
	return f
}

func (f *Fixture) WriteConfig(t *testing.T, change func(map[string]any)) {
	t.Helper()
	cfg := map[string]any{
		"Endpoint":    map[string]any{"Address": "127.0.0.1:0"},
		"GoBootstrap": map[string]any{"Packages": []string{Module + "/records"}},
		"Connector":   "main",
		"Connectors":  []any{map[string]any{"Name": "main", "Driver": "sqlite3", "DSN": f.DSN}},
	}
	if change != nil {
		change(cfg)
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(f.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
}
