package executor

import (
	"github.com/viant/datly/executor/parser"
	"github.com/viant/datly/view"
	"strings"
)

type (
	SqlBuilder struct{}

	SqlData struct {
		SQL  string
		Args []interface{}
	}
)

func NewBuilder() *SqlBuilder {
	return &SqlBuilder{}
}

func (s *SqlBuilder) Build(aView *view.View, paramState *view.ParamState) ([]*SqlData, error) {
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

	result := make([]*SqlData, len(statements))
	if len(statements) == 0 {
		result = append(result, &SqlData{SQL: SQL, Args: params.At(0)})
	}

	for i := range statements {
		result[i] = &SqlData{
			SQL:  statements[i],
			Args: params.At(i),
		}
	}

	return result, nil
}
