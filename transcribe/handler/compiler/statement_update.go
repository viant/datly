package compiler

import (
	"fmt"

	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/velty/ast/expr"
	xshape "github.com/viant/x/shape"
)

// StatementRecordType identifies a named record pointer from resolved authority.
// Mapping, primary keys and payload presence remain SQLX/runtime concerns.
func StatementRecordType(name string) bool {
	ref, err := (xshape.Resolver{}).Reference(name)
	if err != nil || len(ref.Wrappers) != 1 || ref.Wrappers[0].Kind != xshape.WrapperPointer {
		return false
	}
	return !StatementScalarType(name) && ref.Name != "any" && ref.Name != "error" && ref.Name != "interface{}"
}

func compileUpdateStatement(method *expr.Select, authority map[string]plan.StatementSelector) (*plan.StatementPlan, error) {
	call, ok := method.X.(*expr.Call)
	if !ok || call.X != nil || len(call.Args) != 2 {
		return nil, fmt.Errorf("Update requires table and record selectors")
	}
	fields := make([]plan.StatementSelector, 2)
	for i, argument := range call.Args {
		field, err := resolveStatementSelector(argument, authority)
		if err != nil {
			return nil, fmt.Errorf("Update argument %d: %w", i+1, err)
		}
		if field.Path[0] != "Input" {
			return nil, fmt.Errorf("Update arguments must be input selectors")
		}
		fields[i] = field
	}
	if fields[0].Type.Name != "string" {
		return nil, fmt.Errorf("Update table must be an input string")
	}
	if !StatementRecordType(fields[1].Type.Name) {
		return nil, fmt.Errorf("Update row must be a resolved named record pointer")
	}
	return &plan.StatementPlan{Method: "Update", Arguments: fields}, nil
}
