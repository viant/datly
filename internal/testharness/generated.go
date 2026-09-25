package testharness

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	xmodule "github.com/viant/x/module"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
)

// GeneratedModule derives fixture replacements from the actual source checkout,
// so generated tests do not assume a GOPATH layout or a second Datly checkout.
type GeneratedModule struct {
	Path       string
	SourceRoot string
}

func (m GeneratedModule) source() (*modfile.File, string, error) {
	root := m.SourceRoot
	if root == "" {
		_, filename, _, ok := runtime.Caller(0)
		if !ok {
			return nil, "", fmt.Errorf("locate generated fixture source")
		}
		root = filename
	}
	info, err := xmodule.LocateLocal(root)
	if err != nil {
		return nil, "", err
	}
	content, err := os.ReadFile(filepath.Join(info.Dir, "go.mod"))
	if err != nil {
		return nil, "", err
	}
	file, err := modfile.Parse("go.mod", content, nil)
	return file, info.Dir, err
}

func (m GeneratedModule) Content() (string, error) {
	file, root, err := m.source()
	if err != nil {
		return "", err
	}
	original := file.Module.Mod.Path
	modulePath := m.Path
	if modulePath == "" {
		modulePath = "example.com/generated"
	}
	if err = file.AddModuleStmt(modulePath); err != nil {
		return "", err
	}
	for _, replacement := range file.Replace {
		if replacement.New.Version != "" {
			continue
		}
		location := replacement.New.Path
		if !filepath.IsAbs(location) {
			location = filepath.Join(root, location)
		}
		if err = file.AddReplace(replacement.Old.Path, replacement.Old.Version, location, ""); err != nil {
			return "", err
		}
	}
	if err = file.AddRequire(original, "v0.0.0"); err != nil {
		return "", err
	}
	if err = file.AddReplace(original, "", root, ""); err != nil {
		return "", err
	}
	encoded, err := file.Format()
	return string(encoded), err
}

func (m GeneratedModule) DependencyDir(name string) (string, error) {
	file, root, err := m.source()
	if err != nil {
		return "", err
	}
	if file.Module.Mod.Path == name {
		return root, nil
	}
	for _, replacement := range file.Replace {
		if replacement.Old.Path != name || replacement.New.Version != "" {
			continue
		}
		location := replacement.New.Path
		if !filepath.IsAbs(location) {
			location = filepath.Join(root, location)
		}
		return filepath.Clean(location), nil
	}
	// Published dependencies resolve straight from go.mod into the module
	// cache, the same way Go lays it out, without spawning the go tool.
	if location, ok := moduleCacheDir(file, name); ok {
		return location, nil
	}
	// Fallback for selections go.mod alone cannot answer (indirect versions,
	// custom GOFLAGS): ask Go's selected graph.
	command := exec.Command("go", "list", "-mod=readonly", "-m", "-f", "{{.Dir}}", "--", name)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("locate dependency %s: %w: %s", name, err, output)
	}
	location := strings.TrimSpace(string(output))
	if location == "" || !filepath.IsAbs(location) {
		return "", fmt.Errorf("dependency %s has no resolved module directory: %q", name, location)
	}
	return filepath.Clean(location), nil
}

// moduleCacheDir derives GOMODCACHE/<escaped path>@<version> for a module
// required (and not replaced) by the source go.mod.
func moduleCacheDir(file *modfile.File, name string) (string, bool) {
	for _, required := range file.Require {
		if required.Mod.Path != name || required.Mod.Version == "" {
			continue
		}
		escaped, err := module.EscapePath(required.Mod.Path)
		if err != nil {
			return "", false
		}
		cache := os.Getenv("GOMODCACHE")
		if cache == "" {
			gopath := os.Getenv("GOPATH")
			if gopath == "" {
				home, err := os.UserHomeDir()
				if err != nil {
					return "", false
				}
				gopath = filepath.Join(home, "go")
			}
			cache = filepath.Join(filepath.SplitList(gopath)[0], "pkg", "mod")
		}
		location := filepath.Join(cache, escaped+"@"+required.Mod.Version)
		if info, err := os.Stat(location); err == nil && info.IsDir() {
			return location, true
		}
		return "", false
	}
	return "", false
}

func (m GeneratedModule) Write(t testing.TB, root string) {
	t.Helper()
	content, err := m.Content()
	if err != nil {
		t.Fatalf("generated module: %v", err)
	}
	if err = os.WriteFile(filepath.Join(root, "go.mod"), []byte(content), 0644); err != nil {
		t.Fatalf("write generated module: %v", err)
	}
	m.WriteSums(t, root)
}

// WriteSums copies the source checkout's verified dependency sums into a fixture.
// This avoids module verification downloads for unchanged dependency selections.
func (m GeneratedModule) WriteSums(t testing.TB, root string) {
	t.Helper()
	_, source, err := m.source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(source, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "go.sum"), data, 0644); err != nil {
		t.Fatal(err)
	}
}

// GeneratedGoMod retains the existing test entrypoint over the shared owner.
func GeneratedGoMod() string {
	content, err := (GeneratedModule{}).Content()
	if err != nil {
		panic(err)
	}
	return content
}
func WriteGeneratedGoMod(t *testing.T, root string) { t.Helper(); (GeneratedModule{}).Write(t, root) }

// GeneratedGoModWithoutModule is for tests that deliberately verify missing
// module identity; all local dependencies still come from the actual checkout.
func GeneratedGoModWithoutModule() string {
	content := GeneratedGoMod()
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "module ") {
			lines = append(lines[:i], lines[i+1:]...)
			break
		}
	}
	return strings.Join(lines, "\n")
}
