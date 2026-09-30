// Package packageasset discovers and snapshots Go embedded resources.
package packageasset

import (
	"fmt"
	"io/fs"
	"strings"
)

type Resources struct {
	Namespace string
	Files     []string
	// SourceFile and Symbol come from the current Go embed declaration.
	SourceFile string
	Symbol     string
}

func (r *Resources) Validate() error {
	if r == nil {
		return fmt.Errorf("package resources are required")
	}
	if strings.TrimSpace(r.Namespace) == "" || strings.TrimSpace(r.Namespace) != r.Namespace || strings.ContainsAny(r.Namespace, ":/\\") {
		return fmt.Errorf("invalid package resource namespace %q", r.Namespace)
	}
	seen := map[string]bool{}
	for _, file := range r.Files {
		if !fs.ValidPath(file) || file == "." {
			return fmt.Errorf("invalid package resource path %q", file)
		}
		if seen[file] {
			return fmt.Errorf("duplicate package resource path %q", file)
		}
		seen[file] = true
	}
	if len(r.Files) == 0 {
		return fmt.Errorf("package resource files are required")
	}
	return nil
}
