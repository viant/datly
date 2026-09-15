package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestGenExecutableRejectsOuterDistinct(t *testing.T) {
	ctx := context.Background()
	binary := filepath.Join(t.TempDir(), "datly")
	if out, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"get", "patch"} {
		t.Run(op, func(t *testing.T) {
			root := t.TempDir()
			const module = "github.com/viant/datly/distinctprobe"
			(testharness.GeneratedModule{Path: module}).Write(t, root)
			if err := os.Mkdir(filepath.Join(root, "source"), 0755); err != nil {
				t.Fatal(err)
			}
			text := strings.Replace(genpatch.OuterProjectionDQL(module, op, "orders.*", "items.*", false), "SELECT orders.*", "SELECT DISTINCT orders.*", 1)
			if err := os.WriteFile(filepath.Join(root, "source/Orders.dql"), []byte(text), 0644); err != nil {
				t.Fatal(err)
			}
			out, err := exec.CommandContext(ctx, binary, "transcribe", op, "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), module+"/source").CombinedOutput()
			if err == nil || !strings.Contains(string(out), "DISTINCT") {
				t.Fatalf("DISTINCT accepted: %v %s", err, out)
			}
			if _, err := os.Stat(filepath.Join(root, "api")); !os.IsNotExist(err) {
				t.Fatalf("rejected DISTINCT published artifacts: %v", err)
			}
		})
	}
}
