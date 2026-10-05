package transcribe

import (
	"fmt"
	"path/filepath"
	"strings"

	xmodule "github.com/viant/x/module"
)

// Discovery may read several explicitly configured modules. Validation must
// plan each source within its own module, rather than grant every source the
// destination authority of a sibling or the enclosing application module.
func validationSourceRoots(base string, moduleDirs []string) (func(*Source) (string, error), error) {
	type root struct{ directory, module string }
	var roots []root
	for _, directory := range append([]string{base}, moduleDirs...) {
		if !filepath.IsAbs(directory) {
			directory = filepath.Join(base, directory)
		}
		canonical, err := filepath.EvalSymlinks(directory)
		if err != nil {
			return nil, err
		}
		module, err := xmodule.LocateLocal(canonical)
		if err != nil {
			return nil, err
		}
		moduleDir, err := filepath.EvalSymlinks(module.Dir)
		if err != nil {
			return nil, err
		}
		roots = append(roots, root{canonical, moduleDir})
	}
	return func(source *Source) (string, error) {
		directory := source.BaseDir()
		if directory == "" {
			return "", fmt.Errorf("discovered source location is required")
		}
		if !filepath.IsAbs(directory) {
			directory = filepath.Join(base, directory)
		}
		directory, err := filepath.EvalSymlinks(directory)
		if err != nil {
			return "", err
		}
		module, err := xmodule.LocateLocal(directory)
		if err != nil {
			return "", err
		}
		moduleDir, err := filepath.EvalSymlinks(module.Dir)
		if err != nil {
			return "", err
		}
		selected := ""
		for _, candidate := range roots {
			relative, err := filepath.Rel(candidate.directory, directory)
			if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || candidate.module != moduleDir {
				continue
			}
			if len(candidate.directory) > len(selected) {
				selected = candidate.directory
			}
		}
		if selected == "" {
			return "", fmt.Errorf("source %q is outside configured validation module roots", source.Path)
		}
		return selected, nil
	}, nil
}
