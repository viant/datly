// Package packageasset defines the shared generated-package asset descriptor.
// Generation writes it; package loading consumes it without importing emitters.
package packageasset

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

const ManifestName = ".datly-gen.json"

type Manifest struct {
	Resources *Resources           `json:"resources,omitempty"`
	Owners    map[string]*Manifest `json:"owners,omitempty"`
}

type Resources struct {
	Namespace string   `json:"namespace"`
	Files     []string `json:"files"`
}

// Read returns nil when the package has no generated-resource manifest.
func ReadAll(source fs.FS) ([]*Resources, error) {
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
	var result []*Resources
	if manifest.Resources != nil {
		result = append(result, manifest.Resources)
	}
	names := make([]string, 0, len(manifest.Owners))
	for name := range manifest.Owners {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		member := manifest.Owners[name]
		if member == nil || member.Owners != nil {
			return nil, fmt.Errorf("invalid resource owner %q", name)
		}
		if member.Resources != nil {
			result = append(result, member.Resources)
		}
	}
	seen := map[string]bool{}
	for _, r := range result {
		if err := r.Validate(); err != nil {
			return nil, err
		}
		if seen[r.Namespace] {
			return nil, fmt.Errorf("duplicate resource namespace %s", r.Namespace)
		}
		seen[r.Namespace] = true
	}
	return result, nil
}

func Read(source fs.FS) (*Resources, error) {
	all, err := ReadAll(source)
	if err != nil {
		return nil, err
	}
	if len(all) == 0 {
		return nil, nil
	}
	if len(all) > 1 {
		return nil, fmt.Errorf("package has multiple resource owners; use ReadAll")
	}
	return all[0], nil
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
