package compiler

import (
	"fmt"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/velty/ast/expr"
)

// compileMergeStatement resolves the existing SQLX buffered operation without
// introducing SQL classification, row matching, allocation or a merge policy.
func compileMergeStatement(method *expr.Select, authority map[string]plan.StatementSelector) (*plan.StatementPlan, error) {
	call, ok := method.X.(*expr.Call)
	if !ok || call.X != nil || len(call.Args) != 4 {
		return nil, fmt.Errorf("Merge requires table, rows, SQLX config and result selectors")
	}
	fields := make([]plan.StatementSelector, 4)
	for i, arg := range call.Args {
		var err error
		fields[i], err = resolveStatementSelector(arg, authority)
		if err != nil {
			return nil, fmt.Errorf("Merge argument %d: %w", i+1, err)
		}
	}
	if fields[0].Type.Name != "string" || fields[0].Path[0] != "Input" {
		return nil, fmt.Errorf("Merge table must be an input string field")
	}
	if !strings.HasPrefix(fields[1].Type.Name, "[]") || fields[1].Path[0] != "Input" {
		return nil, fmt.Errorf("Merge rows must be an input slice field")
	}
	if fields[2].Type.Name != "MergeConfig" || fields[2].Type.Package != "github.com/viant/sqlx/metadata/info" || fields[2].Path[0] != "Input" {
		return nil, fmt.Errorf("Merge config must be an input SQLX info.MergeConfig field")
	}
	if fields[3].Type.Name != "MergeResult" || fields[3].Type.Package != "github.com/viant/sqlx/metadata/info" || fields[3].Path[0] != "Output" || !fields[3].Addressable {
		return nil, fmt.Errorf("Merge result must be an addressable output SQLX info.MergeResult field")
	}
	return &plan.StatementPlan{Method: "Merge", MergeTable: fields[0], MergeRows: fields[1], MergeConfig: fields[2], MergeResult: fields[3]}, nil
}
