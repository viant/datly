package generate

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"

	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

// HookScaffoldAsset is an optional create-once, user-owned lifecycle file.
// It is emitted by the existing package transaction but never enters the
// generated-file manifest.
type HookScaffoldAsset struct {
	Destination string
	File        *ast.File
	PackagePath string
	EntityHooks []HookScaffoldContract
	Catalog     *typecatalog.Catalog
	evidence    *hookScaffoldEvidence
}

// Clone returns an isolated parser-backed copy.
func (a *HookScaffoldAsset) Clone() (*HookScaffoldAsset, error) {
	if a == nil {
		return nil, nil
	}
	file, err := cloneGoFile(a.File)
	if err != nil {
		return nil, fmt.Errorf("clone hook scaffold AST: %w", err)
	}
	var catalog *typecatalog.Catalog
	if a.Catalog != nil {
		catalog, err = a.Catalog.Clone()
		if err != nil {
			return nil, err
		}
	}
	contracts := append([]HookScaffoldContract(nil), a.EntityHooks...)
	for i := range contracts {
		contracts[i] = contracts[i].clone()
	}
	return &HookScaffoldAsset{Destination: a.Destination, File: file, PackagePath: a.PackagePath, EntityHooks: contracts, Catalog: catalog, evidence: a.evidence.clone()}, nil
}

// HookScaffoldPlan is a validated create-once user-owned file.
type HookScaffoldPlan struct {
	Destination string
	File        *ast.File
	PackagePath string
	EntityHooks []HookScaffoldContract
	Catalog     *typecatalog.Catalog
	evidence    *hookScaffoldEvidence
}

// HookScaffoldContract identifies one lowered role, its source type and mandatory
// methods. Compiler-approved native signatures are retained separately from this
// mutable metadata and the proposal AST.
type HookScaffoldContract struct {
	Type     string
	Identity string
	Path     []string
	Required []string
}

func (c HookScaffoldContract) clone() HookScaffoldContract {
	c.Path = append([]string(nil), c.Path...)
	c.Required = append([]string(nil), c.Required...)
	return c
}

// Filename resolves the create-once destination without requiring emission.
func (a *HookScaffoldAsset) Filename() (string, error) {
	destination := strings.TrimSpace(a.Destination)
	if destination == "" {
		destination = "lifecycle.go"
	}
	relative, err := managedRelativePath(destination)
	if err != nil || filepath.Base(relative) != relative || filepath.Ext(relative) != ".go" {
		return "", fmt.Errorf("hook scaffold destination %q must be a package-local .go file", destination)
	}
	return relative, nil
}

func resolveHookScaffold(plan *Plan, asset *HookScaffoldAsset) error {
	if asset == nil {
		return nil
	}
	copy := *asset
	if override := plan.Generation.File("lifecycle", ""); override != "" {
		copy.Destination = override
	}
	if copy.Destination == "" {
		copy.Destination = plan.Generation.File("lifecycle", "lifecycle.go")
	}
	destination, err := copy.Filename()
	if err != nil {
		return err
	}
	asset, err = asset.Clone()
	if err != nil {
		return err
	}
	plan.HookScaffold = &HookScaffoldPlan{Destination: destination, File: asset.File, PackagePath: asset.PackagePath, EntityHooks: asset.EntityHooks, Catalog: asset.Catalog, evidence: asset.evidence}
	if err := plan.validateHookScaffold(); err != nil {
		plan.HookScaffold = nil
		return err
	}
	return nil
}

// validateHookScaffoldPlan protects both Generator.Plan and the exported
// direct-emission APIs. A caller-provided Plan is not assumed to be trusted.
func (plan *Plan) validateHookScaffold() error {
	if plan == nil || plan.HookScaffold == nil {
		return nil
	}
	hook := plan.HookScaffold
	if hook.File == nil {
		return fmt.Errorf("hook scaffold AST is required")
	}
	genericMutation := plan.Settings.Mutation != ""
	if plan.ContractHandler == nil && plan.MutationHandler == nil && !genericMutation && !plan.ShapesOnly {
		return fmt.Errorf("hook scaffold requires a generated contract handler")
	}
	if plan.MutationHandler == nil && !genericMutation {
		for _, contract := range []ContractPlan{plan.Input, plan.Output} {
			if plan.ShapesOnly && contract.Type == "" {
				continue
			}
			if contract.Ownership != ContractGenerated {
				return fmt.Errorf("hook scaffold requires package-local generated input and output contracts")
			}
			if !isDirectLocalType(contract.Type) {
				return fmt.Errorf("hook scaffold requires direct package-local contract types, got %s", contract.Type)
			}
		}
	}
	relative, err := managedRelativePath(hook.Destination)
	if err != nil {
		return fmt.Errorf("hook scaffold destination: %w", err)
	}
	if filepath.Base(relative) != relative || filepath.Ext(relative) != ".go" {
		return fmt.Errorf("hook scaffold destination %q must be a package-local .go file", hook.Destination)
	}
	if err = validateHandlerComments(hook.File); err != nil {
		return err
	}
	imports, err := handlerImports(hook.File, "")
	if err != nil {
		return err
	}
	if err = validateContractHandlerImports(imports); err != nil {
		return err
	}
	if plan.MutationHandler != nil || genericMutation {
		if err = plan.validateMutationHookScaffold(); err != nil {
			return err
		}
	} else if len(hook.EntityHooks) != 0 {
		return fmt.Errorf("entity hook scaffold requires a mutation handler")
	} else if err = validateHookMethods(hook.File, plan, imports); err != nil {
		return err
	}
	hook.Destination = relative
	return nil
}

func validateHookMethods(file *ast.File, plan *Plan, imports map[string]string) error {
	for _, role := range []struct {
		contract ContractPlan
		method   string
		args     []string
	}{
		{plan.Input, "Init", []string{"context.Context"}},
		{plan.Output, "Finalize", []string{"context.Context", "error"}},
	} {
		if role.contract.Type == "" || !plan.localShape(role.contract.Package) {
			continue
		}
		method, err := hookMethod(file, role.method, role.contract.Type)
		if err != nil {
			return err
		}
		if err = validateHookSignature(method, imports, role.args); err != nil {
			label := "input"
			if role.method == "Finalize" {
				label = "output"
			}
			return fmt.Errorf("%s hook %s: %w", label, role.method, err)
		}
	}
	return nil
}

func validateExistingHookScaffold(path string, plan *Plan) error {
	if plan.MutationHandler != nil || plan.Settings.Mutation != "" {
		return plan.validateExistingMutationHookScaffold(path)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments|parser.AllErrors)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}
	imports, err := handlerImports(file, "")
	if err != nil {
		return err
	}
	for _, role := range []struct {
		contract ContractPlan
		method   string
	}{{plan.Input, "Init"}, {plan.Output, "Finalize"}} {
		if plan.localShape(role.contract.Package) {
			continue
		}
		if method, _ := hookMethod(file, role.method, role.contract.Type); method != nil {
			return fmt.Errorf("authored hook %s.%s belongs in %s; explicit migration required", role.contract.Type, role.method, role.contract.Package)
		}
	}
	if err = validateHookMethods(file, plan, imports); err != nil {
		return fmt.Errorf("lifecycle contract changed: %w", err)
	}
	return nil
}

func hookMethod(file *ast.File, name, receiver string) (*ast.FuncDecl, error) {
	var result *ast.FuncDecl
	for _, declaration := range file.Decls {
		method, ok := declaration.(*ast.FuncDecl)
		if !ok || method.Recv == nil || method.Name == nil || method.Name.Name != name {
			continue
		}
		if len(method.Recv.List) != 1 {
			continue
		}
		pointer, ok := method.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		identifier, ok := pointer.X.(*ast.Ident)
		if !ok || identifier.Name != receiver {
			continue
		}
		if result != nil {
			return nil, fmt.Errorf("hook scaffold method %s on %s is declared more than once", name, receiver)
		}
		result = method
	}
	if result == nil {
		return nil, fmt.Errorf("hook scaffold requires method %s on *%s", name, receiver)
	}
	if result.Body == nil {
		return nil, fmt.Errorf("hook scaffold method %s on *%s requires a body", name, receiver)
	}
	return result, nil
}

func validateHookSignature(method *ast.FuncDecl, imports map[string]string, expected []string) error {
	params := expandedFieldTypes(method.Type.Params)
	results := expandedFieldTypes(method.Type.Results)
	if len(params) != len(expected) || len(results) != 1 {
		return fmt.Errorf("signature has %d parameters and %d results", len(params), len(results))
	}
	for index, expression := range params {
		actual, err := canonicalType(expression, imports, "")
		if err != nil {
			return err
		}
		if actual != expected[index] {
			return fmt.Errorf("parameter %d is %s, expected %s", index+1, actual, expected[index])
		}
	}
	actual, err := canonicalType(results[0], imports, "")
	if err != nil {
		return err
	}
	if actual != "error" {
		return fmt.Errorf("result is %s, expected error", actual)
	}
	return nil
}

func isDirectLocalType(expression string) bool {
	for _, char := range strings.TrimSpace(expression) {
		if !(char == '_' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9') {
			return false
		}
	}
	return token.IsIdentifier(strings.TrimSpace(expression))
}

func hookScaffoldFileText(packageName string, plan *HookScaffoldPlan) (string, error) {
	if plan == nil || plan.File == nil {
		return "", fmt.Errorf("validated hook scaffold plan is required")
	}
	file, err := cloneGoFile(plan.File)
	if err != nil {
		return "", err
	}
	if file == nil || file.Name == nil {
		return "", fmt.Errorf("validated hook scaffold AST is required")
	}
	file.Name.Name = packageName
	source, err := (xshape.SourceParser{}).FormatFile(file)
	if err != nil {
		return "", fmt.Errorf("format hook scaffold: %w", err)
	}
	return string(source), nil
}
