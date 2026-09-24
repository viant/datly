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
