package build

import (
	"fmt"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/mod/module"
)

const DefaultLinkPackage = "internal/dependencylink"

var linkPackageName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// resolveLinkPackage preserves the name -> internal/name shorthand while
// accepting explicit module-relative import directories.
func resolveLinkPackage(value string) (relative, name string, err error) {
	value = strings.TrimSpace(value)
	if value == "" {
		value = DefaultLinkPackage
	} else if !strings.Contains(value, "/") {
		value = "internal/" + value
	}
	if path.Clean(value) != value || !filepath.IsLocal(value) || strings.ContainsAny(value, "\\:") || module.CheckImportPath(value) != nil {
		return "", "", fmt.Errorf("link package must be a module-relative package directory: %q", value)
	}
	for _, part := range strings.Split(value, "/") {
		if part == "." || part == ".." || strings.HasPrefix(part, ".") || part == "vendor" {
			return "", "", fmt.Errorf("invalid link package directory %q", value)
		}
	}
	name = path.Base(value)
	return value, name, nil
}

func validateLinkName(name string) error {
	if !linkPackageName.MatchString(name) || token.Lookup(name).IsKeyword() || name == "_" || name == "main" {
		return fmt.Errorf("invalid link package name %q", name)
	}
	return nil
}

// Reject symlinked destinations (including in-module links) and nested modules.
// Publication additionally uses os.Root so a path swap cannot escape the root.
func validateLinkPath(root, relative string) error {
	if !filepath.IsLocal(relative) {
		return fmt.Errorf("link path is outside the module: %q", relative)
	}
	current := root
	for _, part := range strings.Split(filepath.Clean(relative), string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlinked link path is not supported: %s", current)
		}
		if info.IsDir() {
			if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
				return fmt.Errorf("link path enters a nested module: %s", current)
			} else if !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}
