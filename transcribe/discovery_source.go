package transcribe

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/viant/bindly/resource"
	packageresources "github.com/viant/datly/bootstrap/resources"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/internal/dependency"
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
	staged.prepareDependencySnapshot(ctx, source)
	imports, err := (&dependency.Loader{Workspace: workspace, Catalog: catalog, Registry: staged.Registry, Snapshot: source.dependencySnapshot}).LoadSource(ctx, source.Text, source.Scope)
	if err != nil {
		return nil, err
	}
	assets, err := (packageresources.Loader{Workspace: workspace, Packages: imports, Holders: staged.Holders}).Load(ctx)
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
	if compiledSource.PreparedColumns == nil {
		compiledSource.PreparedColumns = staged.PreparedColumns
	}
	if compiledSource.Resources == nil {
		compiledSource.Resources = assets.Store
	}
	if compiledSource.Resources == nil {
		compiledSource.Resources = resource.New()
	}
	result, err := NewCompiler().Compile(ctx, &compiledSource)
	if err != nil {
		return nil, err
	}
	if err := (&dependency.Loader{Workspace: workspace, Catalog: result.Source.Types, Registry: staged.Registry}).LoadCodecs(ctx, result.Component, result.TypeContext); err != nil {
		return nil, err
	}
	return result, nil
}

func (d *Discovery) prepareDependencySnapshot(ctx context.Context, source *Source) {
	if source.dependencySnapshot == nil {
		prepared := dql.PrepareSource(source.Text)
		if prepared.Directives.Settings != nil && prepared.Directives.Settings.Generation != nil && len(prepared.Directives.Settings.Generation.BorrowedSQLRows) != 0 {
			source.dependencySnapshot = &dependency.Snapshot{}
		}
	}
	if source.dependencySnapshot != nil && source.dependencySnapshot.Select == nil {
		build := source.GoBuild.WithContext(ctx)
		if build.Dir == "" {
			build.Dir = d.BaseDir
		}
		source.dependencySnapshot.Select = func(call context.Context, pkg, dir string) ([]string, error) {
			selected, err := selectedFactoryPackage(call, build.WithContext(call), pkg)
			if err != nil {
				return nil, err
			}
			if filepath.Clean(selected.Dir) != filepath.Clean(dir) {
				return nil, fmt.Errorf("dependency build selection directory mismatch for %s", pkg)
			}
			return append(append([]string(nil), selected.GoFiles...), selected.CgoFiles...), nil
		}
	}
}
