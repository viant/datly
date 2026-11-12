package parse

import (
	"github.com/viant/datly/repository/shape/dql/decl"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
)

// Diagnostic describes parser issue with source position.
type Diagnostic struct {
	Stage   string
	Message string
	Offset  int
	Line    int
	Column  int
}

// FunctionCall captures declaration function invocation.
type FunctionCall struct {
	Name    string
	Args    []string
	Raw     string
	Offset  int
	Line    int
	Column  int
	Handled bool
}

// Result is parser output.
type Result struct {
	Query        *query.Select
	Columns      sqlparser.Columns
	Declarations []*decl.Declaration
	TypeContext  *typectx.Context
	Functions    []*FunctionCall
	Diagnostics  []*Diagnostic
}
