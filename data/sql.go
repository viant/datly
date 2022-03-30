package data

import (
	"github.com/viant/datly/shared"
	"strings"
)

func detectColumnsSQL(v *View) string {
	source := v.Source()
	if strings.Contains(source, string(shared.Criteria)) {
		if v.hasWhereClause {
			source = strings.ReplaceAll(source, string(shared.Criteria), " AND 1 = 0")
		} else {
			source = strings.ReplaceAll(source, string(shared.Criteria), " WHERE 1 = 0")
		}
	}

	if strings.Contains(source, string(shared.ColumnInPosition)) {
		source = strings.ReplaceAll(source, string(shared.ColumnInPosition), " 1 = 0")
	}

	if strings.Contains(source, string(shared.Pagination)) {
		source = strings.ReplaceAll(source, string(shared.Pagination), " ")
	}

	SQL := "SELECT " + v.Alias + ".* FROM " + source + " " + v.Alias + " WHERE 1=0"
	return SQL
}
