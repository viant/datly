package builder

import (
	sqlmacro "github.com/viant/datly/sql/macro"
)

// preparedProjectionSource carries the explicit predicate expansion through
// projection planning. Fallback filters and binding complete the source before
// any result wrapper is rendered.
type preparedProjectionSource struct {
	sql                 string
	macroArgs           []any
	macroCount          int
	parentHandled       bool
	hadRelationCriteria bool
	compositeInjected   bool
}

func (o *builderOptions) prepareProjectionSource() (*preparedProjectionSource, error) {
	result := &preparedProjectionSource{sql: o.sqlText, parentHandled: o.templateParentBindings}
	var err error
	if !o.templateParentBindings && o.skipRelationFilter {
		result.sql, _, err = sqlmacro.StripParentKeyCalls(result.sql)
	} else if !o.templateParentBindings {
		result.sql, result.macroArgs, result.macroCount, err = sqlmacro.ExpandParentKeyCalls(result.sql, o.dialect, o.positionalArgs, o.compositeRows)
		result.parentHandled = result.macroCount > 0
	}
	if err != nil {
		return nil, err
	}
	filter := relationFilter{relation: o.relation, positionalArgs: o.positionalArgs, compositeColumns: o.compositeColumns, compositeRows: o.compositeRows, dialect: o.dialect}
	result.hadRelationCriteria = containsRelationCriteriaToken(result.sql)
	if o.skipRelationFilter {
		result.sql = stripRelationFilterTokens(result.sql)
	} else {
		result.sql = filter.applyCriteriaTokens(result.sql)
		if !result.parentHandled {
			// Resolve authored COLUMN_IN slots, but retain the existing phase of
			// automatic relation predicates at the source scope before result wrapping.
			result.sql, result.compositeInjected = filter.applyColumnIn(result.sql, true)
		}
	}
	result.sql = prepareSelectorCriteriaSQL(result.sql, o.selector)
	return result, nil
}
