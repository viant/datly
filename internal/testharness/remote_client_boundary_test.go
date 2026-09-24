package testharness

import (
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func TestRemoteClientDependencyBoundary(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate boundary test source")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", ".."))
	checkImports := func(packageDir string, forbidden []string) {
		t.Helper()
		root := packageDir
		if !filepath.IsAbs(root) {
			root = filepath.Join(repoRoot, root)
		}
		var violations []string
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			for _, imported := range file.Imports {
				importPath := strings.Trim(imported.Path.Value, "\"")
				for _, prefix := range forbidden {
					if importPath == prefix || strings.HasPrefix(importPath, prefix+"/") {
						violations = append(violations, filepath.ToSlash(relative)+": imports "+importPath)
						break
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", packageDir, err)
		}
		sort.Strings(violations)
		for _, violation := range violations {
			t.Error(violation)
		}
	}

	checkImports("runtime/handler/remote", []string{
		"github.com/viant/mcp/client",
		"github.com/viant/jsonrpc/transport",
		"github.com/viant/datly/internal/client",
		"github.com/viant/datly/transform",
		"crypto/sha256",
		"encoding/json",
		"net/url",
		"reflect",
		"sync",
	})
	checkImports("runtime/remote", []string{
		"github.com/viant/datly/runtime/handler",
		"github.com/viant/datly/internal/client",
		"github.com/viant/mcp/client",
		"github.com/viant/jsonrpc/transport",
	})

	// Resolve the module actually used by this build, including a local replace.
	// Never silently skip the public-contract check for a nonexistent vendor path.
	command := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/viant/xdatly")
	command.Dir = repoRoot
	directory, err := command.Output()
	if err != nil || strings.TrimSpace(string(directory)) == "" {
		t.Fatalf("resolve xdatly module: %v", err)
	}
	checkImports(filepath.Join(strings.TrimSpace(string(directory)), "client"), []string{
		"github.com/viant/datly",
		"github.com/viant/bindly",
		"github.com/viant/mcp/client",
		"github.com/viant/jsonrpc/transport",
	})
}
