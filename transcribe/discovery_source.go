package transcribe

import (
	"context"
	"fmt"

	"github.com/viant/bindly/resource"
	packageresources "github.com/viant/datly/bootstrap/resources"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xmodule "github.com/viant/x/module"
)

// CompileSource compiles one in-memory authored source with the same configured
// workspace/import authority used by Discovery. It is intended for bounded MCP
// authoring surfaces where clients may provide source text but not filesystem
// roots, compiler options, schema refiners, or package selections.
func (d *Discovery) CompileSource(ctx context.Context, source *Source) (*Result, error) {
	if d == nil {
		return nil, fmt.Errorf("transcribe discovery is required")
	}
	if source == nil {
		return nil, ErrNilSource
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	staged := *d
	if staged.Registry != nil {
		registry, err := (x.Cloner{}).Registry(staged.Registry)
		if err != nil {
			return nil, err
		}
		staged.Registry = registry
	}
	catalog := typecatalog.NewCatalog()
	if source.Types != nil {
		var err error
		catalog, err = source.Types.Clone()
		if err != nil {
			return nil, fmt.Errorf("stage source types: %w", err)
		}
	} else if staged.Types != nil {
		var err error
		catalog, err = staged.Types.Clone()
		if err != nil {
			return nil, fmt.Errorf("stage discovery types: %w", err)
		}
	}
	workspace := staged.Workspace
	var err error
	if workspace == nil {
		workspace, err = (xmodule.LocalWorkspace{BaseDir: staged.BaseDir, ModuleDirs: staged.ModuleDirs}).Resolve(ctx)
		if err != nil {
			return nil, err
		}
	}
	imports, err := (&dqlPackageDiscovery{workspace: workspace, catalog: catalog, registry: staged.Registry}).loadSource(ctx, source.Text)
	if err != nil {
		return nil, err
	}
	assets, err := (packageresources.Loader{Workspace: workspace, Packages: imports}).Load(ctx)
	if err != nil {
		return nil, err
	}
	compiledSource := *source
	compiledSource.Types = catalog
	if compiledSource.Connector == "" {
		compiledSource.Connector = staged.Connector
	}
	if compiledSource.ColumnRefiner == nil {
		compiledSource.ColumnRefiner = staged.ColumnRefiner
	}
	if compiledSource.Resources == nil {
		compiledSource.Resources = assets.Store
	}
	if compiledSource.Resources == nil {
		compiledSource.Resources = resource.New()
	}
	return NewCompiler().Compile(ctx, &compiledSource)
}
