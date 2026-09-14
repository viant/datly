// Package packageasset defines the shared generated-package asset descriptor.
// Generation writes it; package loading consumes it without importing emitters.
package packageasset

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"
)

const ManifestName = ".datly-gen.json"

type Manifest struct {
	Resources *Resources `json:"resources,omitempty"`
}

type Resources struct {
	Namespace string   `json:"namespace"`
	Files     []string `json:"files"`
}

// Read returns nil when the package has no generated-resource manifest.
func Read(source fs.FS) (*Resources, error) {
	if source == nil {
		return nil, fmt.Errorf("package filesystem is required")
	}
	data, err := fs.ReadFile(source, ManifestName)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("read %s: %w", ManifestName, err)
	}
	if manifest.Resources == nil {
		return nil, nil
	}
	if err := manifest.Resources.Validate(); err != nil {
		return nil, err
	}
	return manifest.Resources, nil
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
