package golang

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func statementCollectionBody(value *plan.StatementPlan, l *lowerer, sql ast.Expr) ([]ast.Stmt, error) {
	ordinal := *value.CollectionOrdinal
	if value.Method != "Execute" || ordinal < 0 || ordinal >= len(value.Arguments) || value.Arguments[ordinal].Type.Name != "[]int" {
		return nil, fmt.Errorf("invalid collection statement plan")
	}
	for _, path := range []string{"github.com/viant/sqlx", "strings"} {
		alias := l.importsByPath[path]
		if alias == "" {
			alias = l.availableAlias(path[strings.LastIndex(path, "/")+1:])
			l.importsByPath[path] = alias
			l.pathsByAlias[alias] = path
		}
		l.usedImports[path] = true
	}
	var source strings.Builder
	// Selector expressions are validated canonical paths; print their equivalent
	// contract selector without introducing expression evaluation or reflection.
	fieldText := func(field plan.StatementSelector) string {
		root := "input"
		if field.Path[0] == "Output" {
			root = "output"
		}
		return root + "." + strings.Join(field.Path[1:], ".")
	}
	ids := fieldText(value.Arguments[ordinal])
	fmt.Fprintf(&source, "if len(%s)==0 { return %s.Errorf(\"statement collection must not be empty\") }; statementArgs := make([]any,0,%d+len(%s));", ids, l.importsByPath["fmt"], len(value.Arguments)-1, ids)
	for i, field := range value.Arguments {
		if i == ordinal {
			fmt.Fprintf(&source, "for _, statementID := range %s { statementArgs=append(statementArgs,statementID) };", ids)
		} else {
			fmt.Fprintf(&source, "statementArgs=append(statementArgs,%s);", fieldText(field))
		}
	}
	sqlText := strconv.Quote(value.SQL)
	if _, ok := sql.(*ast.Ident); ok {
		sqlText = "statementSQL"
	}
	fmt.Fprintf(&source, "expandedSQL := %s.ParseParameters(%s).RewritePositional(func(ordinal int)string {if ordinal==%d {return %s.TrimSuffix(%s.Repeat(\"?,\",len(%s)),\",\")};return \"?\" }); return capability.Execute(expandedSQL,statementArgs...)", l.importsByPath["github.com/viant/sqlx"], sqlText, ordinal, l.importsByPath["strings"], l.importsByPath["strings"], ids)
	file, err := parser.ParseFile(token.NewFileSet(), "", "package generated;func generated(){"+source.String()+"}", 0)
	if err != nil {
		return nil, err
	}
	return file.Decls[0].(*ast.FuncDecl).Body.List, nil
}
