package generate

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type snapshotFixture struct {
	dir              string
	files, userFiles []EmittedFile
}

func (f *snapshotFixture) init(t *testing.T) {
	t.Helper()
	f.dir = t.TempDir()
	f.files = []EmittedFile{
		{Path: filepath.Join(f.dir, "handler.go"), Content: "package records\nfunc Handle() string {return \"generated\"}\n"},
		{Path: filepath.Join(f.dir, "handlers", "main.velty"), Content: `#set($Output.Value = "generated")`},
		{Path: filepath.Join(f.dir, "router.go"), Content: "package records\nconst Route = \"old\"\n"},
	}
	f.userFiles = []EmittedFile{{Path: filepath.Join(f.dir, "hooks.go"), Content: "package records\nfunc Hook(){}\n"}}
	if err := f.persistence().Commit(); err != nil {
		t.Fatal(err)
	}
}
func (f *snapshotFixture) persistence() *scaffoldPersistence {
	return &scaffoldPersistence{dir: f.dir, owner: "Records", files: append([]EmittedFile(nil), f.files...), userFiles: f.userFiles}
}

func TestStagingEditGuardPreservesLatestUserFiles(t *testing.T) {
	for _, file := range []string{"handler.go", "handlers/main.velty", "hooks.go", "new_notes.txt"} {
		t.Run(file, func(t *testing.T) {
			fixture := &snapshotFixture{}
			fixture.init(t)
			p := fixture.persistence()
			stage := t.TempDir()
			original, _, err := p.copyExisting(fixture.dir, stage)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(fixture.dir, filepath.FromSlash(file))
			if err := os.WriteFile(path, []byte("edit during staging"), 0644); err != nil {
				t.Fatal(err)
			}
			before, err := readScaffoldSnapshot(fixture.dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := p.swap(fixture.dir, stage, original, nil); err == nil || !strings.Contains(err.Error(), "changed during staging") {
				t.Fatalf("swap error=%v", err)
			}
			after, err := readScaffoldSnapshot(fixture.dir)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("latest edit lost during publication")
			}
		})
	}
}
