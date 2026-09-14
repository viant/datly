package sql

import (
	"fmt"
	"io/fs"
	"strings"

	"github.com/viant/datly/spec"
	sqlsource "github.com/viant/sqlparser/source"
)

// ResolveSource expands the resource references of one cloned SQL view source.
// Parsing references remains a transcribe/tag concern; this function only
// materializes the already-typed references into executable SQL.
func ResolveSource(viewName string, source *spec.ViewSource, resources fs.FS) error {
	if source == nil {
		return nil
	}
	uri := strings.TrimSpace(source.URI)
	resolved := source.SQL
	needsURI := strings.TrimSpace(resolved) == "" && uri != ""
	if !needsURI && len(source.Embeds) == 0 {
		return validateSourceStructure(viewName, resolved)
	}
	if resources == nil {
		return fmt.Errorf("resource filesystem is required for view %q", viewName)
	}
	if needsURI {
		body, err := fs.ReadFile(resources, uri)
		if err != nil {
			return fmt.Errorf("read SQL resource %q for view %q: %w", uri, viewName, err)
		}
		resolved = string(body)
	}
	for _, reference := range source.Embeds {
		if reference == nil || strings.TrimSpace(reference.Path) == "" {
			return fmt.Errorf("view %q has an empty SQL resource reference", viewName)
		}
		body, err := fs.ReadFile(resources, reference.Path)
		if err != nil {
			return fmt.Errorf("read SQL resource %q for view %q: %w", reference.Path, viewName, err)
		}
		if strings.TrimSpace(reference.Raw) == "" {
			return fmt.Errorf("view %q inline SQL resource %q has no parser token", viewName, reference.Path)
		}
		if !strings.Contains(resolved, reference.Raw) {
			return fmt.Errorf("view %q SQL does not contain resource token %q", viewName, reference.Raw)
		}
		resolved = strings.ReplaceAll(resolved, reference.Raw, string(body))
	}
	if err := validateSourceStructure(viewName, resolved); err != nil {
		return err
	}
	source.SQL = resolved
	source.Embeds = nil
	return nil
}

func validateSourceStructure(viewName, text string) error {
	if err := sqlsource.ValidateStructure(text); err != nil {
		return fmt.Errorf("SQL source for view %q: %w", viewName, err)
	}
	return nil
}
