package typeidentity

import (
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

// NormalizeDestination retains the exact destination consumed by reader contract
// validation. Callers own detached parameters; codec wire types remain unchanged.
func NormalizeDestination(parameter *spec.Parameter, defaultPackage string, context *spec.TypeContext, lookup xshape.Lookup) error {
	if parameter == nil {
		return nil
	}
	destination := &parameter.OutputTypeExpr
	if *destination == "" && parameter.Codec == nil {
		destination = &parameter.TypeExpr
	}
	if *destination == "" || *destination == "?" {
		return nil
	}
	imports := map[string]string{}
	if context != nil {
		if context.PackagePath != "" {
			defaultPackage = context.PackagePath
		}
		for _, imported := range context.Imports {
			imports[imported.Alias] = imported.Package
		}
	}
	// Without lookup authority, own imports can qualify a destination but an
	// unresolved short name must not be guessed from a default package.
	if lookup == nil {
		defaultPackage = ""
	}
	canonical, err := (xshape.Resolver{Package: defaultPackage, Imports: imports}).Canonical(*destination)
	if err != nil {
		return err
	}
	if lookup == nil {
		*destination = canonical
		return nil
	}
	*destination, err = Alias(canonical, lookup, "contract destination")
	return err
}
