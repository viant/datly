package transcribe

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// existingPrimaryPackageName preserves the ordinary generator's all-production
// file policy. Generated files participate, but their stale bodies need not parse.
// The caller must validate the destination before inspecting its files.
func existingPrimaryPackageName(dir string) (string, error) {
	entries, err := os.ReadDir(dir) // ReadDir returns entries in filename order.
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var name, first string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		filename := filepath.Join(dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return "", fmt.Errorf("generation package file %q: %w", filename, err)
		}
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("generation package file %q is not regular", filename)
		}
		file, err := parser.ParseFile(token.NewFileSet(), filename, nil, parser.PackageClauseOnly)
		if err != nil {
			return "", fmt.Errorf("generation package file %q: %w", filename, err)
		}
		declared := file.Name.Name
		if !token.IsIdentifier(declared) || token.Lookup(declared).IsKeyword() || declared == "_" {
			return "", fmt.Errorf("generation package file %q has invalid Go package name %q", filename, declared)
		}
		if name != "" && name != declared {
			return "", fmt.Errorf("generation package file %q declares %q, conflicting with %q in %q", filename, declared, name, first)
		}
		if name == "" {
			name, first = declared, filename
		}
	}
	return name, nil
}
