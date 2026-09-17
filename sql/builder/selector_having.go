package builder

import (
	"strings"

	sqltext "github.com/viant/sqlparser/source"
	"github.com/viant/sqlx"
)

func (o *builderOptions) appendSelectorCriteria(source string, args []any, explicit bool) (string, []any) {
	if !o.criteriaHaving || explicit {
		return appendAutoSelectorCriteria(source, args, o.selector, explicit)
	}
	// Keep mixed AND/OR criteria intact. Splitting dimension predicates into
	// WHERE could change which groups satisfy an aggregate OR condition.
	having := sqltext.FindTopLevelKeyword(source, "having", 0)
	start := 0
	if having >= 0 {
		start = having + len("having")
	} else if group := sqltext.FindTopLevelKeyword(source, "group by", 0); group >= 0 {
		start = group + len("group by")
	}
	end := start + sqltext.CriteriaBoundary(source[start:])
	argAt := sqlx.ParseParameters(source[:end]).Count()
	prefix := strings.TrimSpace(source[:end])
	if having >= 0 {
		prefix = strings.TrimSpace(source[:having]) + " HAVING (" + strings.TrimSpace(source[start:end]) + ") AND "
	} else {
		prefix += " HAVING "
	}
	resultSQL := prefix + o.selector.Criteria
	if suffix := strings.TrimSpace(source[end:]); suffix != "" {
		resultSQL += " " + suffix
	}
	resultArgs := make([]any, 0, len(args)+len(o.selector.Placeholders))
	resultArgs = append(resultArgs, args[:argAt]...)
	resultArgs = append(resultArgs, o.selector.Placeholders...)
	resultArgs = append(resultArgs, args[argAt:]...)
	return resultSQL, resultArgs
}
