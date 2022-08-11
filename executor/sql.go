package executor

import (
	"github.com/viant/datly/executor/parser"
	"github.com/viant/datly/view"
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

func (s *SqlBuilder) Build(aView *view.View, paramState *view.ParamState) ([]*SQLStatment, error) {
	SQL, params, err := aView.Template.EvaluateSource(paramState.Values, paramState.Has, nil)
	if err != nil {
		return nil, err
	}

	for {
		SQL = strings.TrimSpace(SQL)
		if len(SQL) == 0 || SQL[0] != '(' || SQL[len(SQL)-1] != ')' {
			break
		}

		SQL = SQL[1 : len(SQL)-1]
	}

	statements := parser.ParseWithReader(strings.NewReader(SQL))

	result := make([]*SQLStatment, len(statements))
	if len(statements) == 0 {
		args := params.At(0)
		if len(params.Placeholders) > 0 {
			args = params.Placeholders
		}

		result = append(result, &SQLStatment{SQL: SQL, Args: args})
	}

	for i := range statements {
		result[i] = &SQLStatment{
			SQL:  statements[i],
			Args: params.At(i),
		}
	}

	for _, data := range result {
		var placeholders []interface{}
		sanitizer := &view.CriteriaSanitizer{Placeholders: data.Args}
		expand, err := aView.Expand(&placeholders, data.SQL, &view.Selector{}, view.CommonParams{}, &view.BatchData{}, sanitizer)
		if err != nil {
			return nil, err
		}

		data.SQL = expand
		data.Args = placeholders
	}

	return result, nil
}
