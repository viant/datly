package builder

import (
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqltext "github.com/viant/sqlparser/source"
)

var sourcePaginationToken = sqltext.Token("$PAGINATION")

// Static selector inspection needs a parseable source, without consuming the
// invocation's authored pagination position or treating quoted text as a slot.
func paginationInspectionSource(source string) string {
	return sourcePaginationToken.ReplaceAll(source, "")
}

func prepareSourcePagination(source string, controls *spec.ViewControls) (string, bool) {
	if !sourcePaginationToken.Contains(source) {
		return source, false
	}
	pagination := controls.Clone()
	if pagination != nil {
		pagination.OrderBy = ""
	}
	return dsql.PrepareExecutableSQL(source, pagination), true
}

func remainingSourceControls(controls *spec.ViewControls, paginationConsumed bool) *spec.ViewControls {
	if !paginationConsumed {
		return controls
	}
	result := controls.Clone()
	if result != nil {
		result.Limit, result.Offset = nil, nil
	}
	return result
}
