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
	if value.Method != "Execute" || len(value.CollectionOrdinals) == 0 {
		return nil, fmt.Errorf("invalid collection statement plan")
	}
	collections := make(map[int]bool, len(value.CollectionOrdinals))
	for _, ordinal := range value.CollectionOrdinals {
		if ordinal < 0 || ordinal >= len(value.Arguments) || value.Arguments[ordinal].Type.Name != "[]int" || collections[ordinal] {
			return nil, fmt.Errorf("invalid collection statement plan")
		}
		collections[ordinal] = true
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
	capacity := strconv.Itoa(len(value.Arguments) - len(collections))
	for _, ordinal := range value.CollectionOrdinals {
		ids := fieldText(value.Arguments[ordinal])
		fmt.Fprintf(&source, "if len(%s)==0 { return %s.Errorf(\"statement collection must not be empty\") };", ids, l.importsByPath["fmt"])
		capacity += "+len(" + ids + ")"
	}
	fmt.Fprintf(&source, "statementArgs := make([]any,0,%s);", capacity)
	for i, field := range value.Arguments {
		if collections[i] {
			fmt.Fprintf(&source, "for _, statementID := range %s { statementArgs=append(statementArgs,statementID) };", fieldText(field))
		} else {
			fmt.Fprintf(&source, "statementArgs=append(statementArgs,%s);", fieldText(field))
		}
	}

	sqlText := strconv.Quote(value.SQL)
	if _, ok := sql.(*ast.Ident); ok {
		sqlText = "statementSQL"
	}
	fmt.Fprintf(&source, "expandedSQL := %s.ParseParameters(%s).RewritePositional(func(ordinal int)string {switch ordinal {", l.importsByPath["github.com/viant/sqlx"], sqlText)
	for _, ordinal := range value.CollectionOrdinals {
		fmt.Fprintf(&source, "case %d:return %s.TrimSuffix(%s.Repeat(\"?,\",len(%s)),\",\");", ordinal, l.importsByPath["strings"], l.importsByPath["strings"], fieldText(value.Arguments[ordinal]))
	}
	source.WriteString("};return \"?\" }); return capability.Execute(expandedSQL,statementArgs...)")
	file, err := parser.ParseFile(token.NewFileSet(), "", "package generated;func generated(){"+source.String()+"}", 0)
	if err != nil {
		return nil, err
	}
	return file.Decls[0].(*ast.FuncDecl).Body.List, nil
}
