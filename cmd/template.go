package cmd

import (
	"context"
	"github.com/viant/datly/cmd/ast"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view"
	"strings"
)

func (s *serverBuilder) buildParamView(ctx context.Context, routeOption *option.Route, paramName string, schemaName string, tableParam *option.TableParam, hintsIndex map[string]*option.ParameterHint) (*view.View, error) {
	hint, ok := hintsIndex[paramName]
	paramView := s.buildParamViewWithoutTemplate(paramName, tableParam, schemaName)
	if !ok {
		return paramView, nil
	}

	_, SQL := ast.SplitHint(hint.Hint)
	SQL = strings.TrimSpace(SQL)

	if !ast.IsDataViewKind(hint.Hint) {
		return paramView, nil
	}

	return s.enrichParamViewWithTemplate(ctx, routeOption, SQL, hintsIndex, paramView)
}

func (s *serverBuilder) enrichParamViewWithTemplate(ctx context.Context, routeOption *option.Route, SQL string, hintsIndex map[string]*option.ParameterHint, paramView *view.View) (*view.View, error) {
	aTable, _, err := ParseSQLx(SQL, routeOption, hintsIndex)
	if err != nil {
		return nil, err
	}

	if aTable.SQL == "" {
		aTable.SQL = SQL
	}

	if err = UpdateTableSettings(aTable, routeOption, hintsIndex); err != nil {
		return nil, err
	}

	if err = s.updateView(ctx, aTable, paramView); err != nil {
		return nil, err
	}

	return paramView, nil
}
