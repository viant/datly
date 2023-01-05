package executor

import (
	"github.com/viant/datly/executor/parser"
	"github.com/viant/datly/logger"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/view"
	"github.com/viant/velty/est"
	"strings"
)

type (
	SqlBuilder struct{}

	SQLStatment struct {
		SQL  string
		Args []interface{}
	}
)

func NewBuilder() *SqlBuilder {
	return &SqlBuilder{}
}

func (s *SqlBuilder) Build(aView *view.View, paramState *view.ParamState) (*est.State, []*SQLStatment, *logger.Printer, *expand.SQLCriteria, error) {
	state, params, printer, err := aView.Template.EvaluateState(paramState.Values, paramState.Has, nil, nil)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	SQL := state.Buffer.String()

	for {
		SQL = strings.TrimSpace(SQL)
		if len(SQL) == 0 || SQL[0] != '(' || SQL[len(SQL)-1] != ')' {
			break
		}

		SQL = SQL[1 : len(SQL)-1]
	}

	statements := parser.ParseWithReader(strings.NewReader(SQL))

	result := make([]*SQLStatment, len(statements))
	if len(statements) == 0 && strings.TrimSpace(SQL) != "" {
		result = append(result, &SQLStatment{SQL: SQL, Args: params.At(0)})
	}

	for i := range statements {
		result[i] = &SQLStatment{
			SQL:  statements[i],
			Args: params.At(i),
		}
	}

	for _, data := range result {
		var placeholders []interface{}
		expanded, err := aView.Expand(&placeholders, data.SQL, &view.Selector{}, view.CriteriaParam{}, &view.BatchData{}, params)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		data.SQL = expanded
		data.Args = placeholders
	}

	return state, result, printer, params, nil
}
