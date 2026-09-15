package generate

import (
	"fmt"
	"path/filepath"
	"strings"
)

// validateFilenameSettings also validates controls for optional products, so a
// typo cannot become a write hazard only when that product is later enabled.
func (p *Plan) validateFilenameSettings() error {
	settings := p.Generation
	if settings == nil {
		return nil
	}
	if err := settings.ValidateFilePrefix(); err != nil {
		return err
	}
	for _, role := range []string{"handler", "lifecycle", "mutation", "resources", "links"} {
		if file := settings.File(role, ""); file != "" {
			if _, err := packageGoDestination(file, role); err != nil {
				return err
			}
		}
	}
	for role, file := range settings.SupportFiles {
		if _, err := packageGoDestination(file, "support "+role); err != nil {
			return err
		}
	}
	if file := settings.TemplateFile; file != "" {
		relative, err := managedRelativePath(file)
		if err != nil || relative != file || strings.Contains(file, "\\") || (filepath.Ext(file) != ".velty" && filepath.Ext(file) != ".sql") {
			return fmt.Errorf("template destination %q must be a relative .sql or .velty resource path", file)
		}
	}
	return nil
}
