package developer

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestTranscribeSourceNameNativePublicSupport(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/app"}).Write(t, root)
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT)"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
		t.Fatal(err)
	}
	text := `#package('example.com/app/api/records')
#setting($_ = $route('/records','PATCH'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $Records<[]*Record>(body/data).Cardinality('Many'))
#define($_ = $Data<[]*Record>(output/body))
#define($_ = $Current<[]*CurrentRow>(view/Current).Cardinality('Many').TypeName('CurrentRow') /* SELECT id,label FROM records */)
SELECT r.*,type(r,'Record') FROM records r`
	if err := os.WriteFile(filepath.Join(root, "source", "patch.dql"), []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"transcribe", "patch", "-dir", root, "-source", "patch.dql", "-source-name", "LocationListPatch", "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), "example.com/app/source"}
	run := func() {
		t.Helper()
		var out, diagnostic bytes.Buffer
		if status := Run(ctx, args, &out, &diagnostic); status != 0 {
			t.Fatalf("generation failed: %d %s", status, diagnostic.String())
		}
	}
	snapshot := func() map[string]string {
		t.Helper()
		result := map[string]string{}
		err := filepath.WalkDir(filepath.Join(root, "api"), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				data, e := os.ReadFile(path)
				if e != nil {
					return e
				}
				result[path] = string(data)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	run()
	before := snapshot()
	run()
	if !reflect.DeepEqual(before, snapshot()) {
		t.Fatal("repeated named generation changed artifacts")
	}
	consumer := `package app_test
import ("testing"; "context"; records "example.com/app/api/records")
func TestPublicSupport(t *testing.T){var indexes *records.LocationListPatchHandlerReadIndexes; indexes,_=(&records.Input{}).ReadIndexes(context.Background()); _=indexes;_ = records.LocationListPatchComponent{};_ = records.LocationListPatchDatlyResources}
`
	if err := os.WriteFile(filepath.Join(root, "consumer_test.go"), []byte(consumer), 0600); err != nil {
		t.Fatal(err)
	}
	if output, err := testharness.SourceGoCommand(t, root, "test", "./...").CombinedOutput(); err != nil {
		t.Fatalf("external public symbols: %v\n%s", err, output)
	}
	invalidRoot := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/app"}).Write(t, invalidRoot)
	if err := os.Mkdir(filepath.Join(invalidRoot, "source"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(invalidRoot, "source", "patch.dql"), []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	for i, arg := range args {
		if arg == "-source-name" {
			args[i+1] = "!!!"
			break
		}
	}
	args[3] = invalidRoot
	var out, diagnostic bytes.Buffer
	if status := Run(ctx, args, &out, &diagnostic); status == 0 {
		t.Fatal("invalid generated identifier accepted")
	}
	if _, err := os.Stat(filepath.Join(invalidRoot, "api")); !os.IsNotExist(err) {
		t.Fatal("invalid name published artifacts")
	}

}
