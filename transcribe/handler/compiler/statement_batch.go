package compiler

import (
	"fmt"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/velty/ast/expr"
	"reflect"
	"strings"
)

func compileBatchStatement(method *expr.Select, authority map[string]plan.StatementSelector) (*plan.StatementPlan, error) {
	call, ok := method.X.(*expr.Call)
	if !ok || call.X != nil || (len(call.Args) != 4 && !(method.ID == "Insert" && len(call.Args) == 5)) {
		return nil, fmt.Errorf("%s batch requires table, rows, batch size and affected-row selectors", method.ID)
	}
	fields := make([]plan.StatementSelector, 4)
	for i, arg := range call.Args[:4] {
		var err error
		fields[i], err = resolveStatementSelector(arg, authority)
		if err != nil {
			return nil, fmt.Errorf("batch argument %d: %w", i+1, err)
		}
	}
	if fields[0].Path[0] != "Input" || fields[0].Type.Name != "string" {
		return nil, fmt.Errorf("batch table must be an input string")
	}
	if fields[1].Path[0] != "Input" || !strings.HasPrefix(fields[1].Type.Name, "[]*") {
		return nil, fmt.Errorf("batch rows must be an input pointer slice")
	}
	if fields[2].Path[0] != "Input" || fields[2].Type.Name != "int" {
		return nil, fmt.Errorf("batch size must be an input int")
	}
	if fields[3].Path[0] != "Output" || fields[3].Type.Name != "int64" || !fields[3].Addressable {
		return nil, fmt.Errorf("affected rows must be an addressable output int64")
	}
	var suffix *string
	if len(call.Args) == 5 {
		literal, ok := call.Args[4].(*expr.Literal)
		if !ok || literal.RType != reflect.TypeFor[string]() {
			return nil, fmt.Errorf("batch conflict SQL must be a string literal")
		}
		value := literal.Value
		suffix = &value
	}
	return &plan.StatementPlan{BatchOnDuplicateKeySQL: suffix, Method: method.ID, BatchTable: fields[0], BatchRows: fields[1], BatchSize: fields[2], BatchAffected: fields[3]}, nil
}
