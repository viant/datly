package pipeline

import (
	"reflect"
	"strings"

	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	dqlstmt "github.com/viant/datly/repository/shape/dql/statement"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/sqlparser"
)

func BuildExec(sourceName, sqlText string, statements dqlstmt.Statements) (*plan.View, []*dqlshape.Diagnostic) {
	name := SanitizeName(sourceName)
	if name == "" {
		name = "DQLView"
	}
	tables := statements.DMLTables(sqlText)
	table := name
	if len(tables) > 0 {
		table = tables[0]
	}
	fieldType := reflect.TypeOf([]map[string]interface{}{})
	elementType := reflect.TypeOf(map[string]interface{}{})
	view := &plan.View{
		Path:        name,
		Holder:      name,
		Name:        name,
		Mode:        "SQLExec",
		Table:       table,
		SQL:         sqlText,
		Cardinality: "many",
		FieldType:   fieldType,
		ElementType: elementType,
	}
	return view, ValidateExecStatements(sqlText, statements)
}

func ValidateExecStatements(sqlText string, statements dqlstmt.Statements) []*dqlshape.Diagnostic {
	var result []*dqlshape.Diagnostic
	for _, stmt := range statements {
		if stmt == nil || !stmt.IsExec {
			continue
		}
		body := strings.TrimSpace(sqlText[stmt.Start:stmt.End])
		if body == "" {
			continue
		}
		lower := strings.ToLower(body)
		span := StatementSpan(sqlText, stmt)
		switch {
		case stmt.Kind == dqlstmt.KindService:
			if firstQuoted(body) == "" {
				result = append(result, &dqlshape.Diagnostic{
					Code:     dqldiag.CodeDMLServiceArg,
					Severity: dqlshape.SeverityError,
					Message:  "service DML call is missing quoted table argument",
					Hint:     "use $sql.Insert(\"TABLE\", ...) or $sql.Update(\"TABLE\", ...)",
					Span:     span,
				})
			}
		case strings.HasPrefix(lower, "insert"):
			if _, err := sqlparser.ParseInsert(body); err != nil {
				result = append(result, &dqlshape.Diagnostic{
					Code:     dqldiag.CodeDMLInsert,
					Severity: dqlshape.SeverityError,
					Message:  strings.TrimSpace(err.Error()),
					Hint:     "fix INSERT statement syntax",
					Span:     span,
				})
			}
		case strings.HasPrefix(lower, "update"):
			if _, err := sqlparser.ParseUpdate(body); err != nil {
				result = append(result, &dqlshape.Diagnostic{
					Code:     dqldiag.CodeDMLUpdate,
					Severity: dqlshape.SeverityError,
					Message:  strings.TrimSpace(err.Error()),
					Hint:     "fix UPDATE statement syntax",
					Span:     span,
				})
			}
		case strings.HasPrefix(lower, "delete"):
			if _, err := sqlparser.ParseDelete(body); err != nil {
				result = append(result, &dqlshape.Diagnostic{
					Code:     dqldiag.CodeDMLDelete,
					Severity: dqlshape.SeverityError,
					Message:  strings.TrimSpace(err.Error()),
					Hint:     "fix DELETE statement syntax",
					Span:     span,
				})
			}
		}
	}
	return result
}

func firstQuoted(input string) string {
	index := strings.Index(input, `"`)
	if index == -1 {
		return ""
	}
	tail := input[index+1:]
	end := strings.Index(tail, `"`)
	if end == -1 {
		return ""
	}
	return strings.TrimSpace(tail[:end])
}
