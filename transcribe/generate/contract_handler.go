package generate

import (
	"fmt"
	"go/ast"
	"go/token"
	"path/filepath"
	"strings"

	xshape "github.com/viant/x/shape"
)

const contractHandlerPackage = "github.com/viant/xdatly/handler"

// ContractHandlerAsset is generated binder-aware Go handler source accepted
// from the transcribe Go target. Supplied simple Go functions remain
// GoHandlerAsset; the two products have different public contracts.
type ContractHandlerAsset struct {
	Destination string
	Factory     string
	File        *ast.File
}

// Clone returns an isolated parser-backed copy.
func (a *ContractHandlerAsset) Clone() (*ContractHandlerAsset, error) {
	if a == nil {
		return nil, nil
	}
	file, err := cloneGoFile(a.File)
	if err != nil {
		return nil, fmt.Errorf("clone generated contract handler AST: %w", err)
	}
	return &ContractHandlerAsset{Destination: a.Destination, Factory: a.Factory, File: file}, nil
}

// ContractHandlerPlan is one validated manifest-owned generated Go handler.
type ContractHandlerPlan struct {
	Destination string
	Factory     string
	File        *ast.File
}

func resolveContractHandler(plan *Plan, asset *ContractHandlerAsset, targetPackage string) error {
	if asset == nil {
		return nil
	}
	if asset.File == nil {
		return fmt.Errorf("generated contract handler AST is required")
	}
	if strings.TrimSpace(targetPackage) == "" {
		return fmt.Errorf("generated contract handler target package is required")
	}
	factory := strings.TrimSpace(asset.Factory)
	if !token.IsIdentifier(factory) || !token.IsExported(factory) {
		return fmt.Errorf("generated contract handler factory %q must be an exported Go identifier", factory)
	}
	destination := strings.TrimSpace(asset.Destination)
	if override := plan.Generation.File("handler", ""); override != "" {
		destination = override
	}
	if destination == "" {
		destination = plan.Generation.File("handler", "handler.go")
	}
	relative, err := managedRelativePath(destination)
	if err != nil {
		return fmt.Errorf("generated contract handler destination: %w", err)
	}
	if filepath.Base(relative) != relative || filepath.Ext(relative) != ".go" {
		return fmt.Errorf("generated contract handler destination %q must be a package-local .go file", destination)
	}
	if err = validateHandlerComments(asset.File); err != nil {
		return err
	}
	imports, err := handlerImports(asset.File, targetPackage)
	if err != nil {
		return err
	}
	if err = validateContractHandlerImports(imports); err != nil {
		return err
	}
	function, err := handlerFunction(asset.File, factory)
	if err != nil {
		return err
	}
	if err = validateContractFactory(function, plan, imports, targetPackage); err != nil {
		return fmt.Errorf("generated contract handler %s: %w", factory, err)
	}
	if err = validateHandlerDeclarations(asset.File, factory, plan); err != nil {
		return err
	}
	if plan.Handler != "" && plan.Handler != factory {
		return fmt.Errorf("route handler %q conflicts with generated contract handler factory %q", plan.Handler, factory)
	}
	plan.Handler = factory
	plan.ContractHandler = &ContractHandlerPlan{Destination: relative, Factory: factory, File: asset.File}
	return nil
}

func validateContractHandlerImports(imports map[string]string) error {
	for _, packagePath := range imports {
		packagePath = strings.TrimSpace(packagePath)
		if packagePath == "github.com/viant/datly/exec" ||
			packagePath == "github.com/viant/datly/runtime" ||
			strings.HasPrefix(packagePath, "github.com/viant/datly/runtime/") {
			return fmt.Errorf("generated contract handler cannot import Datly execution package %q", packagePath)
		}
	}
	return nil
}

func validateContractFactory(function *ast.FuncDecl, plan *Plan, imports map[string]string, targetPackage string) error {
	return (handlerFactorySignature{packagePath: contractHandlerPackage, name: "Contract", display: "handler.Contract"}).validate(function, plan, imports, targetPackage)
}

func contractHandlerFileText(packageName string, plan *ContractHandlerPlan) (string, error) {
	if plan == nil || plan.File == nil {
		return "", fmt.Errorf("validated generated contract handler plan is required")
	}
	file, err := cloneGoFile(plan.File)
	if err != nil {
		return "", err
	}
	if file == nil || file.Name == nil {
		return "", fmt.Errorf("validated generated contract handler AST is required")
	}
	file.Name.Name = packageName
	source, err := (xshape.SourceParser{}).FormatFile(file)
	if err != nil {
		return "", fmt.Errorf("format generated contract handler: %w", err)
	}
	return string(source), nil
}
