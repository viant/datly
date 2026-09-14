package generate

import (
	"fmt"
	"go/ast"
	"go/parser"
	"path/filepath"
	"strings"
)

func (plan *Plan) validateGeneratedNames() error {
	if plan == nil {
		return nil
	}
	owners := map[string]string{}
	reserve := func(name, owner string) error {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil
		}
		if previous := owners[name]; previous != "" && previous != owner {
			return fmt.Errorf("generated type %q is shared by %s and %s", name, previous, owner)
		}
		owners[name] = owner
		return nil
	}
	for name, owner := range map[string]string{"Component": "component holder"} {
		if err := reserve(name, owner); err != nil {
			return err
		}
	}
	for _, contract := range []struct {
		name string
		plan ContractPlan
	}{{"input contract", plan.Input}, {"output contract", plan.Output}} {
		if name := localTypeName(contract.plan.Type); name != "" {
			if err := reserve(name, contract.name); err != nil {
				return err
			}
		}
	}
	if name := plan.inputMarkerType(); name != "" {
		if err := reserve(name, "input presence marker"); err != nil {
			return err
		}
	}
	for _, view := range plan.Views {
		name := view.Name
		if view.Ownership == ViewLinked {
			name = localTypeName(view.Type)
		}
		if err := reserve(name, "view "+view.Name); err != nil {
			return err
		}
		if view.Ownership == ViewGenerated && len(view.SetMarkerFields) > 0 {
			if err := reserve(view.Name+"Has", "set marker for view "+view.Name); err != nil {
				return err
			}
		}
	}
	for _, helper := range plan.HelperTypes {
		if err := reserve(helper.Name, "helper "+helper.Name); err != nil {
			return err
		}
	}
	for _, generated := range plan.GeneratedTypes {
		if err := reserve(generated.Name, "generated type "+generated.Name+" at "+generated.Destination); err != nil {
			return err
		}
	}
	for _, name := range plan.referencedPlaceholderTypes() {
		if err := reserve(name, "placeholder "+name); err != nil {
			return err
		}
	}
	if plan.VeltyHandler != nil {
		if err := reserve(plan.VeltyHandler.Factory, "Velty handler factory"); err != nil {
			return err
		}
		if err := reserve(lowerInitial(plan.VeltyHandler.Factory)+"Template", "Velty handler template"); err != nil {
			return err
		}
	}
	if resources := plan.Resources; resources != nil {
		for _, name := range []string{"DatlyResourceNamespace", "DatlyResources"} {
			if err := reserve(name, "SQL resource export"); err != nil {
				return err
			}
		}
	}
	if plan.FactoryLink != nil {
		if err := reserve(plan.FactoryLink.Name, "factory linking export"); err != nil {
			return err
		}
	}
	if plan.FactoryLink != nil && plan.ContractHandler != nil {
		for _, declaration := range plan.ContractHandler.File.Decls {
			for _, name := range declarationNames(declaration) {
				if err := reserve(name, "generated contract handler"); err != nil {
					return err
				}
			}
		}
	}
	if plan.MutationHandler != nil {
		if plan.MutationHandler.File == nil {
			return fmt.Errorf("mutation definition source is required")
		}
		for _, declaration := range plan.MutationHandler.File.Decls {
			for _, name := range declarationNames(declaration) {
				if err := reserve(name, "mutation definition"); err != nil {
					return err
				}
			}
		}
		for _, source := range plan.MutationHandler.Support {
			if source.File == nil {
				return fmt.Errorf("mutation support source is required")
			}
			for _, declaration := range source.File.Decls {
				for _, name := range declarationNames(declaration) {
					if err := reserve(name, "mutation support "+source.Destination); err != nil {
						return err
					}
				}
			}
		}
	}
	if plan.EntitySupport != nil && (plan.MutationHandler != nil || plan.ContractHandler != nil) {
		if plan.EntitySupport.File == nil {
			return fmt.Errorf("entity support source is required")
		}
		for _, declaration := range plan.EntitySupport.File.Decls {
			for _, name := range declarationNames(declaration) {
				if err := reserve(name, "entity support"); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (plan *Plan) inputMarkerType() string {
	if plan == nil || plan.Input.Ownership != ContractGenerated || !hasBodyFields(plan.Input.Fields) {
		return ""
	}
	name := localTypeName(plan.Input.Type)
	if name == "" {
		return ""
	}
	return name + "Has"
}

func localTypeName(expression string) string {
	parsed, err := parser.ParseExpr(strings.TrimSpace(expression))
	if err != nil {
		return ""
	}
	for {
		switch actual := parsed.(type) {
		case *ast.Ident:
			return actual.Name
		case *ast.ParenExpr:
			parsed = actual.X
		case *ast.StarExpr:
			parsed = actual.X
		case *ast.ArrayType:
			parsed = actual.Elt
		default:
			return ""
		}
	}
}

func (plan *Plan) validateGeneratedDestinations() error {
	if plan == nil {
		return nil
	}
	routerDest, err := componentHolderDestination(plan.RouterDest)
	if err != nil {
		return err
	}
	plan.RouterDest = routerDest
	owners := map[string]string{}
	reserve := func(destination, owner string, shareViews bool) error {
		destination = filepath.Clean(strings.TrimSpace(destination))
		if destination == "." || destination == "" {
			return fmt.Errorf("%s destination is required", owner)
		}
		if previous := owners[destination]; previous != "" {
			if shareViews && strings.HasPrefix(previous, "view ") {
				return nil
			}
			return fmt.Errorf("generated destination %q is shared by %s and %s", destination, previous, owner)
		}
		for existing, previous := range owners {
			if strings.HasPrefix(destination, existing+string(filepath.Separator)) ||
				strings.HasPrefix(existing, destination+string(filepath.Separator)) {
				return fmt.Errorf("generated destinations %q (%s) and %q (%s) overlap", existing, previous, destination, owner)
			}
		}
		owners[destination] = owner
		return nil
	}
	generatedViews := 0
	for _, view := range plan.Views {
		if view.Ownership != ViewGenerated {
			continue
		}
		generatedViews++
		if err := reserve(view.Destination, "view "+view.Name, true); err != nil {
			return err
		}
	}
	if generatedViews == 0 && plan.hasViewSupport() {
		if err := reserve(plan.ViewDest, "view support", true); err != nil {
			return err
		}
	}
	if err := reserve(plan.RouterDest, "component holder", false); err != nil {
		return err
	}
	if plan.Input.Ownership == ContractGenerated {
		if err := reserve(plan.Input.Destination, "input contract", false); err != nil {
			return err
		}
	}
	if plan.Output.Ownership == ContractGenerated {
		if err := reserve(plan.Output.Destination, "output contract", false); err != nil {
			return err
		}
	}
	for index := range plan.GeneratedTypes {
		generated := &plan.GeneratedTypes[index]
		destination, err := packageGoDestination(generated.Destination, "generated type "+generated.Name)
		if err != nil {
			return err
		}
		generated.Destination = destination
		if err = reserve(destination, "generated type "+generated.Name, false); err != nil {
			return err
		}
	}
	if plan.GoHandler != nil {
		if err := reserve(plan.GoHandler.Destination, "custom handler", false); err != nil {
			return err
		}
	}
	if plan.ContractHandler != nil {
		if err := reserve(plan.ContractHandler.Destination, "generated contract handler", false); err != nil {
			return err
		}
	}
	if plan.MutationHandler != nil {
		if err := reserve(plan.MutationHandler.Destination, "generated mutation definition", false); err != nil {
			return err
		}
		for _, source := range plan.MutationHandler.Support {
			if err := reserve(source.Destination, "generated mutation support", false); err != nil {
				return err
			}
		}
	}
	if plan.FactoryLink != nil {
		if err := reserve(plan.FactoryLink.Destination, "factory linking export", false); err != nil {
			return err
		}
	}
	if plan.HookScaffold != nil {
		if err := reserve(plan.HookScaffold.Destination, "user hook scaffold", false); err != nil {
			return err
		}
	}
	if plan.VeltyHandler != nil {
		if err := reserve(plan.VeltyHandler.GoDestination, "Velty handler factory", false); err != nil {
			return err
		}
		if err := reserve(plan.VeltyHandler.ResourceDestination, "Velty handler resource", false); err != nil {
			return err
		}
	}
	if plan.EntitySupport != nil {
		if err := reserve(plan.EntitySupport.Destination, "entity support", false); err != nil {
			return err
		}
	}
	if resources := plan.Resources; resources != nil {
		if err := reserve(resources.Destination, "SQL resource filesystem", false); err != nil {
			return err
		}
		for _, file := range resources.Files {
			if err := reserve(file.Path, "SQL resource", false); err != nil {
				return err
			}
		}
	}
	return nil
}

func componentHolderDestination(destination string) (string, error) {
	return packageGoDestination(destination, "component holder")
}

func packageGoDestination(destination, owner string) (string, error) {
	relative, err := managedRelativePath(destination)
	if err != nil {
		return "", fmt.Errorf("%s destination %q must be a package-local .go file: %w", owner, destination, err)
	}
	if filepath.Base(relative) != relative || filepath.Ext(relative) != ".go" {
		return "", fmt.Errorf("%s destination %q must be a package-local .go file", owner, destination)
	}
	return relative, nil
}
