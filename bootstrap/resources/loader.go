// Package resources loads Go-declared package assets into the shared
// Bindly filesystem authority used by compilation and invocation.
package resources

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/packageasset"
	xmodule "github.com/viant/x/module"
)

type Loader struct {
	Workspace *xmodule.Workspace
	Packages  []string
	Holders   []any
}

type Loaded struct {
	Store  *resource.Store
	assets map[string]bool
}

// IsAsset identifies exact embedded files, not filename patterns.
func (l *Loaded) IsAsset(path string) bool { return l != nil && l.assets[filepath.Clean(path)] }

// Load creates a fresh stage-owned store. Missing files, invalid embed declarations
// and namespace collisions fail without mutating any previously published store.
func (l Loader) Load(ctx context.Context) (*Loaded, error) {
	if l.Workspace == nil || ctx == nil {
		return nil, fmt.Errorf("package resource workspace and context are required")
	}
	paths := append([]string(nil), l.Packages...)
	sort.Strings(paths)
	result := &Loaded{Store: resource.New(), assets: map[string]bool{}}
	seen := map[string]bool{}
	namespaces := map[string]string{}
	for _, packagePath := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if seen[packagePath] {
			continue
		}
		seen[packagePath] = true
		location, err := l.Workspace.Package(packagePath)
		if err != nil {
			return nil, err
		}
		if location == nil {
			return nil, fmt.Errorf("resource package %q is not available", packagePath)
		}
		root, err := os.OpenRoot(location.Dir)
		if err != nil {
			return nil, err
		}
		defer root.Close()
		source := root.FS()
		declarations, err := sourceResources(location.Dir)
		if err != nil {
			return nil, fmt.Errorf("package %s embedded resources: %w", packagePath, err)
		}
		// Linked holders supply binary resource authority. A source workspace supplies
		// the generation snapshot so a reload observes current SQL/static assets.
		linked := bootstrap.LinkedResources(l.Holders, packagePath)
		sourceNamespaces := map[string]bool{}
		for _, declaration := range declarations {
			sourceNamespaces[declaration.Namespace] = true
		}
		for namespace, embedded := range linked {
			if sourceNamespaces[namespace] {
				continue
			}
			if previous := namespaces[namespace]; previous != "" {
				return nil, fmt.Errorf("resource namespace %q is declared by both %s and %s", namespace, previous, packagePath)
			}
			namespaces[namespace] = packagePath
			if err := result.Store.Register(namespace, embedded); err != nil {
				return nil, err
			}
		}
		manifests := declarations
		for _, manifest := range manifests {
			if previous := namespaces[manifest.Namespace]; previous != "" {
				return nil, fmt.Errorf("resource namespace %q is declared by both %s and %s", manifest.Namespace, previous, packagePath)
			}
			namespaces[manifest.Namespace] = packagePath
			snapshot, err := (packageasset.Snapshotter{Source: source}).Files(ctx, manifest.Files)
			if err != nil {
				return nil, fmt.Errorf("package %s assets: %w", packagePath, err)
			}
			if err := result.Store.Register(manifest.Namespace, snapshot); err != nil {
				return nil, err
			}
			for _, file := range manifest.Files {
				result.assets[filepath.Join(location.Dir, filepath.FromSlash(file))] = true
			}
		}
	}
	return result, nil
}
