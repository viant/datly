package link

import (
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

// TypeContext combines source location and authored imports for type resolution.
func TypeContext(scope, directory string, authored *spec.TypeContext) *typecatalog.ResolutionContext {
	result := &typecatalog.ResolutionContext{PackageDir: directory, PackagePath: strings.TrimSpace(scope)}

	if authored != nil {
		result.DefaultPackage = authored.DefaultPackage
		for _, item := range authored.Imports {
			result.Imports = append(result.Imports, typecatalog.PackageImport{Alias: item.Alias, Package: item.Package})
		}
	}
	return typecatalog.NormalizeContext(result)
}
