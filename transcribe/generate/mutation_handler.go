package generate

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
)

const mutationHandlerPackage = "github.com/viant/xdatly/handler/mutation"

// MutationHandlerAsset is an explicitly supplied generated policy product.
// Its factory returns public mutation.Definition[I,O], not a custom Contract
// or runtime handler. The linking application uses runtime mutation.New.
type MutationHandlerAsset struct {
	Destination, Factory string
	File                 *ast.File
	Support              []MutationSource
}
type MutationHandlerPlan struct {
	Destination, Factory string
	File                 *ast.File
	Support              []MutationSource
}

func (a *MutationHandlerAsset) Clone() (*MutationHandlerAsset, error) {
	if a == nil {
		return nil, nil
	}
	file, err := cloneGoFile(a.File)
	if err != nil {
		return nil, fmt.Errorf("clone generated mutation definition: %w", err)
	}
	result := &MutationHandlerAsset{Destination: a.Destination, Factory: a.Factory, File: file}
	for _, source := range a.Support {
		cloned, err := source.clone()
		if err != nil {
			return nil, err
		}
		result.Support = append(result.Support, cloned)
	}
	return result, nil
}

func (a *MutationHandlerAsset) resolve(plan *Plan, targetPackage string) error {
	if a == nil {
		return nil
	}
	if a.File == nil {
		return fmt.Errorf("generated mutation definition AST is required")
	}
	if strings.TrimSpace(targetPackage) == "" {
		return fmt.Errorf("generated mutation definition target package is required")
	}
	factory := strings.TrimSpace(a.Factory)
	if !token.IsIdentifier(factory) || !token.IsExported(factory) {
		return fmt.Errorf("mutation definition factory %q must be an exported Go identifier", factory)
	}
	destination := strings.TrimSpace(a.Destination)
	if override := plan.Generation.File("mutation", ""); override != "" {
		destination = override
	}
	if destination == "" {
		destination = plan.Generation.File("mutation", "mutation.go")
	}
	resolved, err := (MutationSource{Destination: destination, File: a.File}).resolve(plan, targetPackage)
	if err != nil {
		return err
	}
	imports, err := handlerImports(resolved.File, targetPackage)
	if err != nil {
		return err
	}
	function, err := handlerFunction(a.File, factory)
	if err != nil {
		return err
	}
	if err = (handlerFactorySignature{packagePath: mutationHandlerPackage, name: "Definition", display: "mutation.Definition"}).validate(function, plan, imports, targetPackage); err != nil {
		return fmt.Errorf("mutation definition %s: %w", factory, err)
	}
	if plan.Handler != "" && plan.Handler != factory {
		return fmt.Errorf("route handler %q conflicts with mutation definition factory %q", plan.Handler, factory)
	}
	plan.Handler = factory
	result := &MutationHandlerPlan{Destination: resolved.Destination, Factory: factory, File: resolved.File}
	for _, source := range a.Support {
		resolved, err := source.resolve(plan, targetPackage)
		if err != nil {
			return err
		}
		result.Support = append(result.Support, resolved)
	}
	plan.MutationHandler = result
	return nil
}

func (p *MutationHandlerPlan) source(packageName string) (string, error) {
	if p == nil || p.File == nil {
		return "", fmt.Errorf("validated mutation definition is required")
	}
	return (MutationSource{File: p.File}).source(packageName)
}
