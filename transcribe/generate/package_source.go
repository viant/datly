package generate

import (
	"io/fs"
	"os"
	"path"
	"strings"
)

// PackageFS preserves the validated Go build selection during metadata reload.
// Other generation paths retain their existing source-loading policy.
func (p *Plan) PackageFS(root, directory string) fs.FS {
	base := os.DirFS(root)
	if p.handlerGoFiles == nil {
		return base
	}
	selected := make(map[string]bool, len(p.handlerGoFiles))
	for _, file := range p.handlerGoFiles {
		selected[file] = true
	}
	return &selectedPackageFS{FS: base, directory: path.Clean(directory), selected: selected}
}

type selectedPackageFS struct {
	fs.FS
	directory string
	selected  map[string]bool
}

func (s *selectedPackageFS) ReadDir(name string) ([]fs.DirEntry, error) {
	entries, err := fs.ReadDir(s.FS, name)
	if err != nil || path.Clean(name) != s.directory {
		return entries, err
	}
	result := make([]fs.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || s.selected[entry.Name()] {
			result = append(result, entry)
		}
	}
	return result, nil
}
