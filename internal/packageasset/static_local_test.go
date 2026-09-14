package packageasset

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/viant/datly/spec"
)

// Exercise native handle traversal while a checked directory name is repeatedly
// replaced by a symlink to a different directory inside its parent authority.
// Merely confining to the volume/parent would allow the secret directory.
func TestStaticLocalConcurrentAuthorityReplacement(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"parent/site": "allowed", "secret/site": "secret"} {
		dir := filepath.Join(base, name)
		if err = os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, "index.html"), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	parent, saved := filepath.Join(base, "parent"), filepath.Join(base, "saved")
	content := &spec.StaticContent{Path: "/site", ContentURL: parent + "/site"}
	check := func(required bool) {
		t.Helper()
		snapshot, err := (StaticSource{}).Snapshot(context.Background(), content)
		if err != nil {
			if required {
				t.Fatal(err)
			}
			return
		}
		body, err := fs.ReadFile(snapshot, "index.html")
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != "allowed" {
			t.Fatalf("unvalidated replacement read: %q", body)
		}
	}
	check(true)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if os.Rename(parent, saved) != nil {
				continue
			}
			_ = os.Symlink("secret", parent)
			_ = os.Remove(parent)
			_ = os.Rename(saved, parent)
		}
	}()
	// Always restore the directory before fixture cleanup, including fatal checks.
	defer func() { close(stop); wg.Wait() }()
	for i := 0; i < 300; i++ {
		check(false)
	}
}
