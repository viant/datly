package generate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func forestTemplate(dir, owner, name string) *scaffoldPersistence {
	return &scaffoldPersistence{dir: dir, owner: owner, files: []EmittedFile{{Path: filepath.Join(dir, name), Content: generatedHeader(owner) + "package sample\n"}}}
}

func TestPreparedForestInterleavingAndPublicationPrefix(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "missing", "a")
	child := filepath.Join(parent, "models")
	other := filepath.Join(root, "outside", "b")
	templates := []*scaffoldPersistence{forestTemplate(child, "Child", "child.go"), forestTemplate(other, "Other", "other.go"), forestTemplate(parent, "Parent", "parent.go")}
	forests, reads, files, err := prepareScaffoldForests(templates)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupScaffoldForests(forests)
	if len(forests) != 2 || forests[0].target != parent || forests[1].target != other {
		t.Fatalf("forest order: %+v", forests)
	}
	if len(files) != 3 || files[0].Path != templates[0].files[0].Path || files[2].Path != templates[2].files[0].Path {
		t.Fatalf("result order: %+v", files)
	}
	for i, target := range []string{child, other, parent} {
		if _, err := os.Stat(target); !os.IsNotExist(err) {
			t.Fatalf("preparation exposed %s: %v", target, err)
		}
		for _, f := range forests {
			if withinScaffoldTree(target, f.stage) {
				t.Fatal("stage inside authored root")
			}
		}
		if _, err := os.Stat(reads[i]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "missing")); !os.IsNotExist(err) {
		t.Fatalf("preparation created parent: %v", err)
	}
	// Real publication failure on the later forest's parent preserves the first
	// published forest, including its later-authored parent member.
	if err := os.WriteFile(filepath.Join(root, "outside"), []byte("block publication"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := publishScaffoldForests(forests); err == nil {
		t.Fatal("accepted blocked publication")
	}
	for _, file := range []string{filepath.Join(child, "child.go"), filepath.Join(parent, "parent.go")} {
		if _, err := os.Stat(file); err != nil {
			t.Fatalf("published prefix missing: %v", err)
		}
	}
	if !forests[0].published || forests[1].published {
		t.Fatal("untruthful prefix")
	}
	for _, f := range forests {
		if _, err := os.Stat(f.stage); !os.IsNotExist(err) {
			t.Fatalf("stage leaked: %s %v", f.stage, err)
		}
	}
}

func TestPreparedForestExternalChildEditIsPreserved(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "api")
	child := filepath.Join(parent, "models")
	if err := os.MkdirAll(child, 0755); err != nil {
		t.Fatal(err)
	}
	user := filepath.Join(child, "authored.txt")
	if err := os.WriteFile(user, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	forests, _, _, err := prepareScaffoldForests([]*scaffoldPersistence{forestTemplate(parent, "Parent", "parent.go"), forestTemplate(child, "Child", "child.go")})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupScaffoldForests(forests)
	if err := os.WriteFile(user, []byte("latest external content"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := publishScaffoldForests(forests); err == nil || !strings.Contains(err.Error(), "changed during staging") {
		t.Fatalf("external edit admitted: %v", err)
	}
	got, err := os.ReadFile(user)
	if err != nil || string(got) != "latest external content" {
		t.Fatalf("lost edit: %q %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(parent, "parent.go")); !os.IsNotExist(err) {
		t.Fatalf("published rejected forest: %v", err)
	}
}

func TestPreparedForestOwnedMissingParentsCleanedOnFailure(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "new", "nested", "api")
	forests, _, _, err := prepareScaffoldForests([]*scaffoldPersistence{forestTemplate(target, "Sample", "sample.go")})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupScaffoldForests(forests)
	// Missing stage creates a real rename failure after external parents have
	// been created. Only these still-empty owned parents may be removed.
	if err := os.RemoveAll(forests[0].stage); err != nil {
		t.Fatal(err)
	}
	if err := publishScaffoldForests(forests); err == nil {
		t.Fatal("accepted missing stage")
	}
	if _, err := os.Stat(filepath.Join(root, "new")); !os.IsNotExist(err) {
		t.Fatalf("owned empty parent leaked: %v", err)
	}
}
