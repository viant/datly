package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestGenExecutableRequiresPackage(t *testing.T) {
	ctx := context.Background()
	binary := filepath.Join(t.TempDir(), "datly")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build datly: %v\n%s", err, output)
	}
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE RECORDS(ID INTEGER PRIMARY KEY,NAME TEXT)"); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"get", "patch", "post", "put"} {
		for _, tc := range []struct {
			name, header string
			valid        bool
		}{
			{"missing", "", false}, {"imports_only", `#import('dto','example.com/generated/models')`, false},
			{"blank", `#package('')`, false}, {"malformed", `#package(123)`, false},
			{"outside", `#package('../outside')`, false},
			{"authored", fmt.Sprintf("#package('api/%s')", operation), true},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				root := t.TempDir()
				testharness.WriteGeneratedGoMod(t, root)
				dir := filepath.Join(root, "source")
				if err := os.Mkdir(dir, 0755); err != nil {
					t.Fatal(err)
				}
				source := tc.header + "\n" + fmt.Sprintf("#setting($_ = $route('/records','%s'))\nSELECT ID,NAME FROM RECORDS", strings.ToUpper(operation))
				file := filepath.Join(dir, "Records.dql")
				if err := os.WriteFile(file, []byte(source), 0644); err != nil {
					t.Fatal(err)
				}
				command := exec.CommandContext(ctx, binary, "gen", "-dir", root, "-op", operation, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), "example.com/generated/source")
				output, err := command.CombinedOutput()
				if tc.valid {
					if err != nil {
						t.Fatalf("gen %s: %v\n%s", operation, err, output)
					}
					if _, err := os.Stat(filepath.Join(root, "api", operation, "records_router.go")); err != nil {
						t.Fatal(err)
					}
					if _, err := os.Stat(filepath.Join(root, "generated")); !os.IsNotExist(err) {
						t.Fatal("GEN used implicit destination", err)
					}
				} else {
					if err == nil {
						t.Fatal("gen accepted missing/invalid package", string(output))
					}
					entries, err := os.ReadDir(root)
					if err != nil || len(entries) != 3 {
						t.Fatalf("rejected CLI wrote files: %v %v", entries, err)
					}
					if (tc.name == "missing" || tc.name == "imports_only") && !strings.Contains(string(output), "explicit #package") {
						t.Fatalf("wrong diagnostic: %s", output)
					}
				}
				after, err := os.ReadFile(file)
				if err != nil || string(after) != source {
					t.Fatal("CLI changed DQL source", err)
				}
			})
		}
	}
}
