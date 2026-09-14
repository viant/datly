package generate

import (
	"fmt"
	"go/ast"
	"go/parser"
)

// handlerFactorySignature validates generated public generic factory contracts
// through the existing canonical type authority, never by identifier suffix.
type handlerFactorySignature struct{ packagePath, name, display string }

func (s handlerFactorySignature) validate(function *ast.FuncDecl, plan *Plan, imports map[string]string, targetPackage string) error {
	params, results := expandedFieldTypes(function.Type.Params), expandedFieldTypes(function.Type.Results)
	if len(params) != 0 || len(results) != 1 {
		return fmt.Errorf("factory signature must be func() %s[%s, %s]", s.display, plan.Input.Type, plan.Output.Type)
	}
	actual, err := canonicalType(results[0], imports, targetPackage)
	if err != nil {
		return err
	}
	expectedImports := plan.importMap()
	input, err := parser.ParseExpr(plan.Input.Type)
	if err != nil {
		return fmt.Errorf("parse input contract type: %w", err)
	}
	output, err := parser.ParseExpr(plan.Output.Type)
	if err != nil {
		return fmt.Errorf("parse output contract type: %w", err)
	}
	canonicalInput, err := canonicalType(input, expectedImports, targetPackage)
	if err != nil {
		return err
	}
	canonicalOutput, err := canonicalType(output, expectedImports, targetPackage)
	if err != nil {
		return err
	}
	expected := s.packagePath + "." + s.name + "[" + canonicalInput + "," + canonicalOutput + "]"
	if actual != expected {
		return fmt.Errorf("factory result %q does not match %q", actual, expected)
	}
	return nil
}
