package developer

import (
	"bytes"
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTranscribeNativePreservesOriginalConvenienceAccessor(t *testing.T) {
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
	dql := `#package('example.com/app/api/records')
#setting($_ = $route('/records','POST'))
#define($_ = $Records<[]*Record>(body/data).Cardinality('Many'))
#define($_ = $Data<[]*Record>(output/body))
SELECT r.*,type(r,'Record') FROM records r`
	if err := os.WriteFile(filepath.Join(root, "source", "records.dql"), []byte(dql), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"transcribe", "post", "-dir", root, "-source", "records.dql", "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), "example.com/app/source"}
	run := func() (int, string) {
		var out, diagnostic bytes.Buffer
		status := Run(ctx, args, &out, &diagnostic)
		return status, diagnostic.String()
	}
	if status, message := run(); status != 0 {
		t.Fatalf("initial generation: %s", message)
	}
	target := filepath.Join(root, "api", "records")
	authored := `package records
func(r *Record)SetLabel(value string){r.Label=&value;if r.Has==nil{r.Has=&RecordHas{}};r.Has.Label=true}
func OriginalCall(){r:=&Record{};r.SetLabel("")}
`
	path := filepath.Join(target, "record.go")
	if err := os.WriteFile(path, []byte(authored), 0600); err != nil {
		t.Fatal(err)
	}
	if status, message := run(); status != 0 {
		t.Fatalf("original accessor: %s", message)
	}
	snapshot := func() map[string][]byte {
		t.Helper()
		out := map[string][]byte{}
		err := filepath.WalkDir(target, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			out[path], err = os.ReadFile(path)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	stable := snapshot()
	if status, message := run(); status != 0 {
		t.Fatalf("stable generation: %s", message)
	}
	after := snapshot()
	for path, data := range stable {
		if !bytes.Equal(data, after[path]) {
			t.Fatalf("regeneration changed %s", path)
		}
	}
	if err := os.WriteFile(path, []byte(strings.Replace(authored, "r.Label=&value", "r.Label=value", 1)), 0600); err != nil {
		t.Fatal(err)
	}
	before := snapshot()
	if status, message := run(); status == 0 || !strings.Contains(message, "cannot use") {
		t.Fatalf("invalid accessor accepted: %d %s", status, message)
	}
	after = snapshot()
	if len(before) != len(after) {
		t.Fatal("rejected generation changed files")
	}
	for path, data := range before {
		if !bytes.Equal(data, after[path]) {
			t.Fatalf("rejected generation changed %s", path)
		}
	}
}
