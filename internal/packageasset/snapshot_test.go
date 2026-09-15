package packageasset

import (
	"context"
	"io/fs"
	"runtime"
	"testing"
	"testing/fstest"
)

func TestResourceSnapshotPathsAndIsolation(t *testing.T) {
	source := fstest.MapFS{"a.txt": &fstest.MapFile{Data: []byte("a")}, "z/deep/query.sql": &fstest.MapFile{Data: []byte("SELECT 1")}}
	snapshot, err := (Snapshotter{Source: source}).All(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	source["z/deep/query.sql"].Data = []byte("SELECT 2")
	for _, tt := range []struct{ name, want string }{{"a.txt", "a"}, {"z/deep/query.sql", "SELECT 1"}} {
		actual, err := fs.ReadFile(snapshot, tt.name)
		if err != nil || string(actual) != tt.want {
			t.Fatalf("%s=%q error=%v", tt.name, actual, err)
		}
	}
	if _, err := fs.ReadDir(snapshot, "z"); err != nil {
		t.Fatal(err)
	}
	next, err := (Snapshotter{Source: source}).Files(context.Background(), []string{"z/deep/query.sql"})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := fs.ReadFile(next, "z/deep/query.sql")
	if err != nil || string(actual) != "SELECT 2" {
		t.Fatalf("new generation=%q error=%v", actual, err)
	}
	if _, err := fs.ReadFile(next, "a.txt"); err == nil {
		t.Fatal("unselected asset exposed")
	}
}

func TestResourceSnapshotRejectsInvalidInputs(t *testing.T) {
	source := fstest.MapFS{"file": &fstest.MapFile{Data: []byte("data")}}
	for _, paths := range [][]string{{"../file"}, {"file", "file"}, {"file", "file/child"}, {"missing"}, {"."}} {
		if _, err := (Snapshotter{Source: source}).Files(context.Background(), paths); err == nil {
			t.Fatalf("invalid snapshot accepted: %v", paths)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (Snapshotter{Source: source}).All(ctx); err == nil {
		t.Fatal("cancellation ignored")
	}
}

func TestResourceSnapshotOverlayAndGC(t *testing.T) {
	source := fstest.MapFS{"go.mod": &fstest.MapFile{Data: []byte("module old")}, "kept.go": &fstest.MapFile{Data: []byte("package p")}}
	data := []byte("module current")
	snapshot, err := (Snapshotter{Source: source, Overlay: []File{{Path: "go.mod", Data: data}, {Path: "hook.go", Data: []byte("package p")}}}).All(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	data[0] = 'X'
	source["kept.go"].Data[0] = 'X'
	for iteration := 0; iteration < 10; iteration++ {
		runtime.GC()
		for name, want := range map[string]string{"go.mod": "module current", "kept.go": "package p", "hook.go": "package p"} {
			got, err := fs.ReadFile(snapshot, name)
			if err != nil || string(got) != want {
				t.Fatalf("snapshot %s: %q %v", name, got, err)
			}
		}
	}
	for _, overlay := range [][]File{{{Path: "../escape"}}, {{Path: "x"}, {Path: "x"}}, {{Path: "x"}, {Path: "x/y"}}} {
		if _, err := (Snapshotter{Overlay: overlay}).All(context.Background()); err == nil {
			t.Fatal("invalid overlay accepted")
		}
	}
}
