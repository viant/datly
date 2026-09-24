package generate

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/x"
)

func (r *planResolver) resolveContractOwnership() error {
	if err := r.resolveContract("input", &r.plan.Input, r.input.Contracts.Input); err != nil {
		return err
	}
	return r.resolveContract("output", &r.plan.Output, r.input.Contracts.Output)
}

func (r *planResolver) resolveContract(role string, contract *ContractPlan, reference *ContractReference) error {
	if reference == nil {
		if !isTypeIdentifier(contract.Type) {
			return fmt.Errorf("generated %s contract type %q must be a local Go identifier", role, contract.Type)
		}
		if strings.TrimSpace(contract.Destination) == "" {
			return fmt.Errorf("generated %s contract destination is required", role)
		}
		return nil
	}
	if r.types == nil {
		return fmt.Errorf("linked %s contract requires type authority", role)
	}
	key := strings.TrimSpace(reference.DescriptorKey)
	if key == "" {
		return fmt.Errorf("linked %s contract descriptor key is required", role)
	}
	descriptor, err := r.types.Descriptor(key)
	if err != nil {
		return fmt.Errorf("resolve linked %s contract %q: %w", role, key, err)
	}
	if descriptor == nil || strings.TrimSpace(descriptor.Name) == "" || strings.TrimSpace(descriptor.PkgPath) == "" {
		return fmt.Errorf("linked %s contract %q is not a named package type", role, key)
	}
	expression, err := r.linkedContractExpression(reference.Expression, descriptor)
	if err != nil {
		return fmt.Errorf("resolve linked %s contract: %w", role, err)
	}
	contract.Type = expression
	contract.Ownership = ContractLinked
	contract.DescriptorKey = key
	return nil
}

func (r *planResolver) linkedContractExpression(expression string, descriptor *x.Type) (string, error) {
	typeName := linkedNamedTypeExpression(r.plan, r.input.TargetPackage, descriptor)
	return replaceContractBase(expression, typeName)
}

func linkedNamedTypeExpression(plan *Plan, targetPackage string, descriptor *x.Type) string {
	if descriptor == nil {
		return ""
	}
	typeName := descriptor.Name
	if strings.TrimSpace(targetPackage) == strings.TrimSpace(descriptor.PkgPath) {
		return typeName
	}
	alias := uniqueImportAlias(plan, descriptor.PkgPath)
	ensureImport(plan, alias, descriptor.PkgPath)
	return alias + "." + typeName
}

func replaceContractBase(expression, replacement string) (string, error) {
	parsed, err := parser.ParseExpr(strings.TrimSpace(expression))
	if err != nil {
		return "", fmt.Errorf("parse contract expression %q: %w", expression, err)
	}
	replaced, err := replaceContractNode(parsed, replacement)
	if err != nil {
		return "", err
	}
	var buffer bytes.Buffer
	if err = printer.Fprint(&buffer, token.NewFileSet(), replaced); err != nil {
		return "", err
	}
	return buffer.String(), nil
}

func replaceContractNode(expression ast.Expr, replacement string) (ast.Expr, error) {
	switch actual := expression.(type) {
	case *ast.ParenExpr:
		nested, err := replaceContractNode(actual.X, replacement)
		if err != nil {
			return nil, err
		}
		return &ast.ParenExpr{X: nested}, nil
	case *ast.StarExpr:
		nested, err := replaceContractNode(actual.X, replacement)
		if err != nil {
			return nil, err
		}
		return &ast.StarExpr{X: nested}, nil
	case *ast.ArrayType:
		nested, err := replaceContractNode(actual.Elt, replacement)
		if err != nil {
			return nil, err
		}
		return &ast.ArrayType{Len: actual.Len, Elt: nested}, nil
	case *ast.Ident, *ast.SelectorExpr:
		return parser.ParseExpr(replacement)
	default:
		return nil, fmt.Errorf("unsupported linked contract expression %T", expression)
	}
}

func isTypeIdentifier(value string) bool {
	expression, err := parser.ParseExpr(strings.TrimSpace(value))
	if err != nil {
		return false
	}
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name != ""
}

func (plan *Plan) contractImports() []spec.ImportSpec {
	if plan == nil {
		return nil
	}
	fields := make([]Field, 0, 2)
	fields = append(fields, Field{Type: plan.Input.Type})
	fields = append(fields, Field{Type: plan.Output.Type})
	return importsForFields(fields, plan.Imports)
}

// contractType emits canonical cross-package ownership on the route holder.
// Local aliases remain available to accepted handler source, but are not the
// contract's package authority during discovery and reload.
func (plan *Plan) contractType(contract ContractPlan) string {
	if contract.Ownership == ContractGenerated && !plan.localShape(contract.Package) {
		return uniqueImportAlias(plan, contract.Package) + "." + contract.Type
	}
	return contract.Type
}

func (plan *Plan) holderImports() []spec.ImportSpec {
	return importsForFields([]Field{{Type: plan.contractType(plan.Input)}, {Type: plan.contractType(plan.Output)}, {Type: plan.FactoryExpression}}, plan.Imports)
}
