package sql

import (
	"strings"

	"github.com/viant/datly/spec"
)

var removableSelectCalls = map[string]bool{
	// Projection-only select calls: stripped during authored SQL normalization,
	// but they do not map onto ViewControls fields.
	"allow_nulls": true,
	"cardinality": true,

	spec.ViewControlCacheWarmup:  true,
	spec.ViewControlOrderBy:      true,
	spec.ViewControlSetLimit:     true,
	spec.ViewControlUseCache:     true,
	spec.ViewControlUseConnector: true,
}

func NormalizeAuthoredSQL(sqlText string) string {
	sqlText = unwrapProjectionSQL(strings.TrimSpace(sqlText))
	if sqlText == "" {
		return sqlText
	}
	normalized := normalizeSelectProjection(sqlText)
	return strings.TrimSpace(normalized)
}
