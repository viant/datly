package transcribe

import (
	"context"
	"fmt"
	"path"
	"strings"

	xmodule "github.com/viant/x/module"
)

// discoveryFiles is a file inventory for one compilation, before generation
// writes anything. Workspace.Walk remains the authority for workspace roots,
// build selection, nested modules, symlinks and .goignore.
type discoveryFiles []xmodule.File

func discoverFiles(ctx context.Context, workspace *xmodule.Workspace, include, exclude []string) (discoveryFiles, error) {
	var files discoveryFiles
	err := workspace.Walk(ctx, include, exclude, func(file xmodule.File) error { files = append(files, file); return nil })
	return files, err
}

func (files discoveryFiles) selectFiles(ctx context.Context, include, exclude []string) ([]xmodule.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(include) == 0 {
		return nil, fmt.Errorf("package include patterns are required")
	}
	for _, patterns := range [][]string{include, exclude} {
		for _, pattern := range patterns {
			if strings.TrimSpace(pattern) == "" {
				return nil, fmt.Errorf("empty package pattern")
			}
			if _, err := path.Match(strings.TrimSuffix(pattern, "/..."), ""); err != nil {
				return nil, fmt.Errorf("invalid package pattern %q: %w", pattern, err)
			}
		}
	}
	matches := func(patterns []string, pkg string) bool {
		for _, pattern := range patterns {
			if pattern == "..." {
				return true
			}
			if strings.HasSuffix(pattern, "/...") {
				prefix := strings.TrimSuffix(pattern, "/...")
				if pkg == prefix || strings.HasPrefix(pkg, prefix+"/") {
					return true
				}
			} else if matched, _ := path.Match(pattern, pkg); matched {
				return true
			}
		}
		return false
	}
	var selected []xmodule.File
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if matches(include, file.ImportPath) && !matches(exclude, file.ImportPath) {
			selected = append(selected, file)
		}
	}
	return selected, nil
}
