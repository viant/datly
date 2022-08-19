package cmd

import (
	"context"
	"github.com/viant/datly/cmd/ast"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/sanitizer"
	"github.com/viant/datly/view"
	"strings"
)

func (s *serverBuilder) buildParamView(ctx context.Context, routeOption *option.Route, paramName string, schemaName string, tableParam *option.TableParam, hints option.ParameterHints) (*view.View, error) {
	hintsIndex := hints.Index()
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

	return s.enrichParamViewWithTemplate(ctx, routeOption, SQL, paramView, hints)
}

func (s *serverBuilder) enrichParamViewWithTemplate(ctx context.Context, routeOption *option.Route, SQL string, paramView *view.View, hints option.ParameterHints) (*view.View, error) {
	aTable, _, err := ParseSQLx(SQL, routeOption, hints)
	if err != nil {
		return nil, err
	}

	if aTable.SQL == "" {
		aTable.SQL = SQL
	}

	aTable.SQL = sanitizer.Sanitize(SQL, hints)

	if err = UpdateTableSettings(aTable, routeOption, hints); err != nil {
		return nil, err
	}

	if err = s.updateView(ctx, aTable, paramView); err != nil {
		return nil, err
	}

	return paramView, nil
}
