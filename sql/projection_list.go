package sql

import (
	"strings"
	"unicode"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	sqltext "github.com/viant/sqlparser/source"
)

// ParseProjectionList inspects only a SELECT list, not statement validity.
// Plain identifier lists use the native identifier grammar. Computed items,
// literals and unusual spellings retain the complete SELECT parser's semantics.
func ParseProjectionList(projection string) (query.List, error) {
	if list, ok := identifierProjectionList(sqltext.SplitTopLevelCSV(projection)); ok {
		return list, nil
	}
	parsed, err := sqlparser.ParseQuery("SELECT " + projection + " FROM criteria_source")
	if err != nil || parsed == nil {
		return nil, err
	}
	return parsed.List, nil
}

func identifierProjectionList(parts []string) (query.List, bool) {
	if len(parts) == 0 {
		return nil, false
	}
	if kind, _ := splitSelectionKind(parts[0]); kind != "" {
		return nil, false
	}
	list := make(query.List, 0, len(parts))
	for _, part := range parts {
		core, alias := sqltext.SplitTopLevelAlias(strings.TrimSpace(part))
		if strings.ContainsAny(core, "'\"`[") || strings.IndexFunc(core, unicode.IsSpace) >= 0 || strings.ContainsAny(alias, "'\"`[") ||
			strings.EqualFold(core, "true") || strings.EqualFold(core, "false") || strings.EqualFold(core, "null") {
			return nil, false
		}
		if _, err := sqlparser.TableIdentifierParts(core); err != nil {
			return nil, false
		}
		if alias != "" {
			if names, err := sqlparser.TableIdentifierParts(alias); err != nil || len(names) != 1 {
				return nil, false
			}
		}
		list = append(list, &query.Item{Expr: &expr.Ident{Name: core}, Alias: alias})
	}
	return list, true
}
