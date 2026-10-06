package generate

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScaffoldParentCleanupPreservesReplacementIdentity(t *testing.T) {
	for _, kind := range []string{"regular file", "empty directory", "nonempty owned", "empty owned"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "parent")
			if err := os.Mkdir(path, 0755); err != nil {
				t.Fatal(err)
			}
			info, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			owned := []scaffoldCreatedParent{{path: path, identity: info}}
			switch kind {
			case "regular file", "empty directory":
				// Renaming retains the original inode and prevents its reuse by the new
				// directory: the replacement identity is deterministically distinct.
				if err := os.Rename(path, filepath.Join(root, "original")); err != nil {
					t.Fatal(err)
				}
				if kind == "regular file" {
					err = os.WriteFile(path, []byte("external replacement"), 0600)
				} else {
					err = os.Mkdir(path, 0755)
				}
				if err != nil {
					t.Fatal(err)
				}
			case "nonempty owned":
				if err := os.WriteFile(filepath.Join(path, "external.txt"), []byte("preserve"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			cleanupScaffoldParents(owned)
			_, err = os.Lstat(path)
			if kind == "empty owned" {
				if !os.IsNotExist(err) {
					t.Fatalf("owned empty directory retained: %v", err)
				}
			} else if err != nil {
				t.Fatalf("deleted external replacement/nonempty directory: %v", err)
			}
			if kind == "regular file" {
				content, err := os.ReadFile(path)
				if err != nil || string(content) != "external replacement" {
					t.Fatalf("replacement altered: %q %v", content, err)
				}
			}
		})
	}
}
