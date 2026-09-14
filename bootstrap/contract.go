package bootstrap

import (
	"fmt"
	"reflect"
	"strings"

	xshape "github.com/viant/x/shape"
)

func packageContractExpression(role, generic, tagged string) (string, error) {
	generic = strings.TrimSpace(generic)
	tagged = strings.TrimSpace(tagged)
	if generic == "" {
		return tagged, nil
	}
	if tagged == "" {
		return generic, nil
	}
	equal, err := (xshape.Contract{}).Equivalent(generic, tagged)
	if err != nil {
		return "", fmt.Errorf("compare package %s contract: %w", role, err)
	}
	if !equal {
		return "", fmt.Errorf("component tag %s contract %q conflicts with Component contract %q", role, tagged, generic)
	}
	return generic, nil
}

// ValidateContractTypes verifies that linked types are the exact contracts
// declared by this holder's Component[I,O] arguments.
func (s *RouteSource) ValidateContractTypes(inputType, outputType reflect.Type) error {
	if s == nil {
		return fmt.Errorf("package route source is required")
	}
	imports := make(map[string]string, len(s.Imports))
	for _, item := range s.Imports {
		imports[strings.TrimSpace(item.Alias)] = strings.TrimSpace(item.Package)
	}
	validator := xshape.Contract{Packages: xshape.Packages{
		Default: strings.TrimSpace(s.PackagePath), Imports: imports,
	}}
	for _, contract := range []struct {
		role       string
		expression string
		actual     reflect.Type
	}{
		{role: "input", expression: s.InputType, actual: inputType},
		{role: "output", expression: s.OutputType, actual: outputType},
	} {
		if contract.actual == nil {
			return fmt.Errorf("linked %s type is required", contract.role)
		}
		if err := validator.ValidateLinked(contract.expression, contract.actual); err != nil {
			return fmt.Errorf("linked %s type %s does not match holder contract %q: %w", contract.role, contract.actual, contract.expression, err)
		}
	}
	return nil
}
