// Package devapp provides linked SQLite application fixtures for developer MCP tests.
package devapp

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"os"
	"path/filepath"
	"testing"
)

type Fixture struct {
	Root, DSN string
	DB        *sqlite.Harness
}

func New(t *testing.T) *Fixture {
	t.Helper()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: Module}).Write(t, root)
	for _, dir := range []string{"reader", "writer"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0755); err != nil {
			t.Fatal(err)
		}
		(testharness.GeneratedModule{Path: Module + "/" + dir}).Write(t, filepath.Join(root, dir))
	}
	dsn := filepath.Join(root, "data.db")
	db := sqlite.New(t, sqlite.WithDSN(dsn))
	if err := db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO records VALUES(1,'first')"); err != nil {
		t.Fatal(err)
	}
	return &Fixture{Root: root, DSN: dsn, DB: db}
}
