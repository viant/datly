package transcribe

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/packageasset"
)

// compilePackages includes selected Go-only contracts after DQL overlays have
// claimed their package identities. Imported dependency packages are not exposed.
func (c *discoveryCompilation) compilePackages(ctx context.Context, project *ProjectGeneration, compiled map[string]bool) error {
	identities := make([]string, 0, len(c.packages))
	for identity := range c.packages {
		if !compiled[identity] {
			identities = append(identities, identity)
		}
	}
	sort.Strings(identities)
	for _, identity := range identities {
		if err := ctx.Err(); err != nil {
			return err
		}
		pkg := c.packages[identity]
		route := pkg.Routes[0]
		resources, err := c.packageResources(ctx, route.Dir)
		if err != nil {
			return err
		}
		source := &Source{Scope: route.PackagePath, Name: pkg.ComponentName(), Path: route.SourceFile,
			Connector: c.discovery.Connector, Resources: resources, Types: c.catalog, ColumnRefiner: c.discovery.ColumnRefiner}
		result, err := (&descriptorPackageCompilation{source: source, packageSource: pkg, catalog: c.catalog}).compile(ctx)
		if err != nil {
			return fmt.Errorf("compile package component %q: %w", identity, err)
		}
		project.Components = append(project.Components, result)
	}
	return nil
}

func (c *discoveryCompilation) packageResources(ctx context.Context, dir string) (*resource.Store, error) {
	if existing := c.defaults[dir]; existing != nil {
		return existing, nil
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	snapshot, err := (packageasset.Snapshotter{Source: root.FS()}).All(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot package resources for %q: %w", dir, err)
	}
	resources, err := c.resources.WithDefault(snapshot)
	if err != nil {
		return nil, err
	}
	c.defaults[dir] = resources
	return resources, nil
}
