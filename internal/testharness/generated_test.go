package testharness

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/mod/modfile"
)

func TestGeneratedModuleUsesSourceReplacements(t *testing.T) {
	for _, name := range []string{"source", "source with spaces"} {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), name)
			if err := os.Mkdir(root, 0755); err != nil {
				t.Fatal(err)
			}
			content := "module example.com/source\ngo 1.25\nrequire example.com/local v0.0.0\nreplace example.com/local => ../local\nreplace example.com/versioned => example.com/other v1.2.3\n"
			if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte(content), 0644); err != nil {
				t.Fatal(err)
			}
			fixture := GeneratedModule{Path: "example.com/fixture", SourceRoot: root}
			generated, err := fixture.Content()
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := modfile.Parse("go.mod", []byte(generated), nil)
			if err != nil {
				t.Fatal(err)
			}
			if parsed.Module.Mod.Path != "example.com/fixture" {
				t.Fatal("fixture module name was not changed")
			}
			var local, source, versioned bool
			for _, r := range parsed.Replace {
				switch r.Old.Path {
				case "example.com/local":
					local = r.New.Path == filepath.Join(filepath.Dir(root), "local")
				case "example.com/source":
					source = r.New.Path == root
				case "example.com/versioned":
					versioned = r.New.Path == "example.com/other" && r.New.Version == "v1.2.3"
				}
			}
			if !local || !source || !versioned {
				t.Fatalf("replacements lost: %s", generated)
			}
			location, err := fixture.DependencyDir("example.com/local")
			if err != nil || !filepath.IsAbs(location) {
				t.Fatalf("dependency=%s err=%v", location, err)
			}
		})
	}
	if strings.Contains(GeneratedGoModWithoutModule(), "module example.com/generated") {
		t.Fatal("invalid-module fixture still declares module")
	}
}

func TestGeneratedModuleResolvesSelectedSDK(t *testing.T) {
	fixture := GeneratedModule{}
	source, root, err := fixture.source()
	if err != nil {
		t.Fatal(err)
	}
	var expected *modfile.Replace
	for _, replacement := range source.Replace {
		if replacement.Old.Path == "github.com/viant/xdatly" {
			expected = replacement
		}
	}
	content, err := fixture.Content()
	if err != nil {
		t.Fatal(err)
	}
	file, err := modfile.Parse("go.mod", []byte(content), nil)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, replacement := range file.Replace {
		if replacement.Old.Path == "github.com/viant/xdatly" {
			found = true
			if expected == nil {
				t.Fatal("published SDK unexpectedly replaced in generated application")
			}
			path := expected.New.Path
			if expected.New.Version == "" && !filepath.IsAbs(path) {
				path = filepath.Join(root, path)
			}
			if replacement.New.Path != path || replacement.New.Version != expected.New.Version {
				t.Fatalf("SDK replacement diverged from source: %+v", replacement.New)
			}
		}
	}
	if expected != nil && !found {
		t.Fatal("selected SDK replacement lost in generated application")
	}
	location, err := fixture.DependencyDir("github.com/viant/xdatly")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(location, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "module github.com/viant/xdatly\n") {
		t.Fatalf("resolved wrong SDK: %s", location)
	}
}

func TestGeneratedModuleExplicitTestSDKSource(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "go.mod"), []byte("module example.test/source\ngo 1.25\n"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path, module string
		valid              bool
	}{
		{"correct", filepath.Join(t.TempDir(), "sdk"), "github.com/viant/xdatly", true},
		{"wrong module", filepath.Join(t.TempDir(), "other"), "example.test/other", false},
		{"relative", "relative-sdk", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.module != "" {
				if err := os.Mkdir(tc.path, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(tc.path, "go.mod"), []byte("module "+tc.module+"\ngo 1.25\n"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("DATLY_TEST_XDATLY_DIR", tc.path)
			fixture := GeneratedModule{SourceRoot: source}
			content, err := fixture.Content()
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
			if !tc.valid {
				return
			}
			if !strings.Contains(content, tc.path) {
				t.Fatal("explicit source mapping missing")
			}
			actual, err := fixture.DependencyDir("github.com/viant/xdatly")
			if err != nil || actual != tc.path {
				t.Fatalf("dependency source%s err%v", actual, err)
			}
			unchanged, err := os.ReadFile(filepath.Join(source, "go.mod"))
			if err != nil || strings.Contains(string(unchanged), "replace") {
				t.Fatal("fixture selection mutated source module")
			}
		})
	}
}

func TestSourceGoCommandUsesExplicitSDKWithoutChangingSourceModule(t *testing.T) {
	source := t.TempDir()
	sdk := t.TempDir()
	original := []byte("module example.test/source\ngo 1.25\n")
	if err := os.WriteFile(filepath.Join(source, "go.mod"), original, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "go.sum"), nil, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sdk, "go.mod"), []byte("module github.com/viant/xdatly\ngo 1.25\n"), 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATLY_TEST_XDATLY_DIR", sdk)
	command := SourceGoCommand(t, source, "build", "./cmd/example")
	if command.Dir != source || len(command.Args) != 4 || !strings.HasPrefix(command.Args[2], "-modfile=") {
		t.Fatalf("source build mapping:%v", command.Args)
	}
	content, err := os.ReadFile(strings.TrimPrefix(command.Args[2], "-modfile="))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), sdk) || !strings.Contains(string(content), "module example.test/source") {
		t.Fatal("temporary source module authority changed")
	}
	after, err := os.ReadFile(filepath.Join(source, "go.mod"))
	if err != nil || string(after) != string(original) {
		t.Fatal("tracked source module changed")
	}
}

func TestGeneratedModuleUsesSelectedModfile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/source\ngo 1.25\n"), 0644); err != nil {
		t.Fatal(err)
	}
	selected := filepath.Join(t.TempDir(), "selected.mod")
	if err := os.WriteFile(selected, []byte("module example.com/source\ngo 1.25\nrequire example.com/dependency v0.0.0\nreplace example.com/dependency => example.com/selected v1.2.3\n"), 0644); err != nil {
		t.Fatal(err)
	}
	content, err := (GeneratedModule{SourceRoot: root, SourceModFile: selected}).Content()
	if err != nil || !strings.Contains(content, "example.com/selected v1.2.3") {
		t.Fatalf("selected module: %s %v", content, err)
	}
	if err := os.WriteFile(selected, []byte("module example.com/other\ngo 1.25\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = (GeneratedModule{SourceRoot: root, SourceModFile: selected}).Content(); err == nil {
		t.Fatal("mismatched selected source accepted")
	}
}
