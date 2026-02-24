package compile

import (
	"reflect"
	"strings"

	"github.com/viant/datly/repository/shape"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/sqlparser"
)

func applyColumnDiscoveryPolicy(result *plan.Result, compileOptions *shape.CompileOptions) []*dqlshape.Diagnostic {
	if result == nil {
		return nil
	}
	mode := normalizeColumnDiscoveryMode(shape.CompileColumnDiscoveryAuto)
	if compileOptions != nil {
		mode = normalizeColumnDiscoveryMode(compileOptions.ColumnDiscoveryMode)
	}

	var diags []*dqlshape.Diagnostic
	for _, item := range result.Views {
		if item == nil || !isQueryLikeMode(item.Mode) {
			continue
		}
		required := mode == shape.CompileColumnDiscoveryOn
		if requiresColumnDiscovery(item) {
			required = true
		}
		item.ColumnsDiscovery = required
		if !required {
			continue
		}
		result.ColumnsDiscovery = true
		if mode == shape.CompileColumnDiscoveryOff {
			diags = append(diags, &dqlshape.Diagnostic{
				Code:     dqldiag.CodeColDiscoveryReq,
				Severity: dqlshape.SeverityError,
				Message:  "column discovery is required but disabled",
				Hint:     "enable column discovery or declare an explicit shape/type without wildcard projection",
				Span: dqlshape.Span{
					Start: dqlshape.Position{Line: 1, Char: 1},
					End:   dqlshape.Position{Line: 1, Char: 1},
				},
			})
		}
	}
	return diags
}

func normalizeColumnDiscoveryMode(mode shape.CompileColumnDiscoveryMode) shape.CompileColumnDiscoveryMode {
	switch mode {
	case shape.CompileColumnDiscoveryAuto, shape.CompileColumnDiscoveryOn, shape.CompileColumnDiscoveryOff:
		return mode
	default:
		return shape.CompileColumnDiscoveryAuto
	}
}

func isQueryLikeMode(mode string) bool {
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return true
	}
	return strings.EqualFold(mode, "SQLQuery")
}

func requiresColumnDiscovery(item *plan.View) bool {
	if item == nil {
		return false
	}
	if usesWildcardSQL(item.SQL, item.Table) {
		return true
	}
	return !hasConcreteShape(item)
}

func hasConcreteShape(item *plan.View) bool {
	if item == nil {
		return false
	}
	rType := item.ElementType
	if rType == nil {
		rType = item.FieldType
	}
	if rType == nil {
		return false
	}
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array {
		rType = rType.Elem()
	}
	return rType.Kind() == reflect.Struct
}

func usesWildcardSQL(sqlText, table string) bool {
	if strings.TrimSpace(sqlText) == "" {
		return strings.TrimSpace(table) != ""
	}
	lower := strings.ToLower(sqlText)
	if !strings.Contains(lower, "*") {
		return false
	}
	if !strings.HasPrefix(strings.TrimSpace(lower), "select") && !strings.HasPrefix(strings.TrimSpace(lower), "with") {
		return true
	}
	parsed, err := sqlparser.ParseQuery(sqlText)
	if err != nil {
		return true
	}
	return sqlparser.NewColumns(parsed.List).IsStarExpr()
}
