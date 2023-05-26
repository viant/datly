package executor

import (
	"fmt"
	"github.com/viant/datly/executor/parser"
	"github.com/viant/datly/template/expand"
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

func (s *SqlBuilder) Build(aView *view.View, paramState *view.ParamState) (*expand.State, []*SQLStatment, error) {
	state, err := aView.Template.EvaluateState(paramState.Values, paramState.Has, nil, nil)
	if err != nil {
		return state, nil, err
	}

	SQL := state.Buffer.String()

	if strings.Contains(SQL, "#set") {
		fmt.Printf("??? %v %T %+v\n", SQL, paramState.Values, paramState.Values)
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
	if len(statements) == 0 && strings.TrimSpace(SQL) != "" {
		result = append(result, &SQLStatment{SQL: SQL, Args: state.DataUnit.At(0)})
	}

	for i := range statements {
		result[i] = &SQLStatment{
			SQL:  statements[i],
			Args: state.DataUnit.At(i),
		}
	}

	for _, data := range result {
		if strings.Contains(data.SQL, "#set") {
			fmt.Printf("--- %v %T %+v\n", data.SQL, paramState.Values, paramState.Values)
		}
		var placeholders []interface{}
		expanded, err := aView.Expand(&placeholders, data.SQL, &view.Selector{}, view.CriteriaParam{}, &view.BatchData{}, state.DataUnit)
		if err != nil {
			return nil, nil, err
		}

		data.SQL = expanded
		data.Args = placeholders
	}

	return state, result, nil
}
