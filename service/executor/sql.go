package executor

import (
	"fmt"
	expand2 "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/service/executor/extension"
	"github.com/viant/datly/service/executor/parser"
	"github.com/viant/datly/view"
	"github.com/viant/structology"
	"strings"
)

type (
	SqlBuilder struct{}
)

func NewBuilder() *SqlBuilder {
	return &SqlBuilder{}
}

func (s *SqlBuilder) Build(aView *view.View, templateState *structology.State, session *extension.Session, dataUnit *expand2.DataUnit) (*expand2.State, []*expand2.SQLStatment, error) {

	fmt.Printf("SOURCE: %s\n", aView.Template.Source)
	fmt.Printf("templateState: %T \n%+v\n", templateState.State(), templateState.State())

	state, err := aView.Template.EvaluateStateWithSession(templateState, nil, nil, session, dataUnit)
	if err != nil {
		return state, nil, err
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

	result := make([]*expand2.SQLStatment, len(statements))
	if len(statements) == 0 && strings.TrimSpace(SQL) != "" {
		result = append(result, &expand2.SQLStatment{SQL: SQL, Args: state.DataUnit.At(0)})
	}

	for i := range statements {
		result[i] = &expand2.SQLStatment{
			SQL:  statements[i],
			Args: state.DataUnit.At(i),
		}
	}

	for _, data := range result {
		var placeholders []interface{}
		expanded, err := aView.Expand(&placeholders, data.SQL, &view.Statelet{}, view.CriteriaParam{}, &view.BatchData{}, state.DataUnit)
		if err != nil {
			return nil, nil, err
		}

		data.SQL = expanded
		data.Args = placeholders
	}

	return state, result, nil
}
