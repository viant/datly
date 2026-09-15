package typecatalog

import (
	"fmt"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	xmodule "github.com/viant/x/module"
)

// DestinationAuthority confines authored generation packages to one project
// and its enclosing module. Type lookup authority never grants write authority.
type DestinationAuthority struct{ root, moduleDir, modulePath string }
type Destination struct{ Directory, ImportPath, Name string }

func NewDestinationAuthority(root string) (*DestinationAuthority, error) {
	if strings.TrimSpace(root) == "" {
		return nil, fmt.Errorf("generation project root is required")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	absolute, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, err
	}
	module, err := xmodule.LocateLocal(absolute)
	if err != nil {
		return nil, err
	}
	dir, err := filepath.EvalSymlinks(module.Dir)
	if err != nil {
		return nil, err
	}
	return &DestinationAuthority{root: absolute, moduleDir: dir, modulePath: module.Path}, nil
}

func (a *DestinationAuthority) Package(authored, fallback string) (Destination, error) {
	value := strings.TrimSpace(authored)
	var directory string
	if value == "" {
		directory = fallback
	} else {
		if strings.ContainsAny(value, "\\:") || strings.HasPrefix(value, "/") {
			return Destination{}, fmt.Errorf("invalid generation package %q", value)
		}
		for _, part := range strings.Split(value, "/") {
			if part == ".." || part == "." || part == "" {
				return Destination{}, fmt.Errorf("generation package %q escapes or has ambiguous path", value)
			}
		}
		if value == a.modulePath || strings.HasPrefix(value, a.modulePath+"/") {
			relative := strings.TrimPrefix(strings.TrimPrefix(value, a.modulePath), "/")
			var err error
			directory, err = filepath.Rel(a.root, filepath.Join(a.moduleDir, filepath.FromSlash(relative)))
			if err != nil {
				return Destination{}, err
			}
		} else if strings.Contains(strings.Split(value, "/")[0], ".") {
			return Destination{}, fmt.Errorf("generation package %q is outside project module %q", value, a.modulePath)
		} else {
			directory = filepath.FromSlash(value)
		}
	}
	if directory == "" {
		directory = "."
	}
	if filepath.IsAbs(directory) || directory == ".." || strings.HasPrefix(directory, ".."+string(filepath.Separator)) {
		return Destination{}, fmt.Errorf("generation package %q escapes project root", value)
	}
	if directory != "." && !fs.ValidPath(filepath.ToSlash(directory)) {
		return Destination{}, fmt.Errorf("invalid generation package directory %q", directory)
	}
	current := a.root
	for _, part := range strings.Split(filepath.ToSlash(directory), "/") {
		if part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return Destination{}, err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return Destination{}, fmt.Errorf("generation destination %q contains symlink or non-directory", directory)
		}
		if _, err = os.Stat(filepath.Join(current, "go.mod")); err == nil && current != a.moduleDir {
			return Destination{}, fmt.Errorf("generation destination %q crosses a nested module", directory)
		}
	}
	imported, err := xmodule.ImportPathLocal(a.moduleDir, a.modulePath, filepath.Join(a.root, directory))
	if err != nil {
		return Destination{}, err
	}
	name := path.Base(imported)
	if !token.IsIdentifier(name) || token.Lookup(name).IsKeyword() || name == "_" {
		return Destination{}, fmt.Errorf("generation package %q has invalid Go package name %q", value, name)
	}
	return Destination{Directory: directory, ImportPath: imported, Name: name}, nil
}
