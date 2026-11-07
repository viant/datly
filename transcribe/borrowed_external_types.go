package transcribe

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/internal/dependency"
	"github.com/viant/datly/typecatalog"
	xmodule "github.com/viant/x/module"
)

// Supplement explicit foreign imports with the application's selected module
// sources. Reflection cannot retain Go aliases. Local generated owner declarations
// keep their separate fresh-DQL authority and are not selected as build roots.
func loadBorrowedExternalTypes(ctx context.Context, source *Source, local *xmodule.Workspace, catalog *typecatalog.Catalog, imports map[string]bool) error {
	if source.GoBuild == nil {
		return nil
	}
	var patterns []string
	for path := range imports {
		location, err := local.Package(path)
		if err != nil {
			return err
		}
		if location == nil {
			patterns = append(patterns, path)
		}
	}
	if len(patterns) == 0 {
		return nil
	}
	sort.Strings(patterns)
	build, overlay, err := authoredInputBuild(ctx, source.GoBuild)
	if err != nil {
		return err
	}
	if overlay {
		return fmt.Errorf("borrowed external type authority cannot use an unsealed Go overlay")
	}
	selection, err := (xmodule.BuildWorkspace{BaseDir: build.Dir, Patterns: patterns, Tags: build.Tags, Env: append(os.Environ(), build.Env...)}).Resolve(ctx)
	if err != nil {
		return fmt.Errorf("select borrowed external type authority: %w", err)
	}
	external := map[string]bool{}
	workspace := selection.Workspace()
	for _, path := range patterns {
		location, err := workspace.Package(path)
		if err != nil {
			return err
		}
		if location != nil {
			external[path] = true
		}
	}
	_, err = (&dependency.Loader{Workspace: workspace, Catalog: catalog, Snapshot: source.dependencySnapshot}).LoadImports(ctx, external)
	return err
}

// Initial borrowed discovery needs the same source alias authority before plan
// generation starts. Ordinary non-borrowed discovery is unchanged.
func loadBorrowedExternalSourceTypes(ctx context.Context, source *Source, workspace *xmodule.Workspace, catalog *typecatalog.Catalog) error {
	if source.dependencySnapshot == nil {
		return nil
	}
	imports := map[string]bool{}
	if err := addBorrowedExternalSourceImports(source.Text, imports); err != nil {
		return err
	}
	return loadBorrowedExternalTypes(ctx, source, workspace, catalog, imports)
}

func addBorrowedExternalSourceImports(text string, imports map[string]bool) error {
	prepared := dql.PrepareSource(text)
	if err := prepared.Err(); err != nil {
		return err
	}
	if prepared.TypeContext != nil {
		for _, imp := range prepared.TypeContext.Imports {
			imports[imp.Package] = true
		}
	}
	return nil
}
