package dml

import (
	"github.com/viant/datly/repository/shape/compile/pipeline"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	dqlstmt "github.com/viant/datly/repository/shape/dql/statement"
	"github.com/viant/datly/repository/shape/plan"
)

// Compile builds an exec-oriented view and validates DML statements.
func Compile(sourceName, sqlText string, statements dqlstmt.Statements) (*plan.View, []*dqlshape.Diagnostic) {
	return pipeline.BuildExec(sourceName, sqlText, statements)
}
