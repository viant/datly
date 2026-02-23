package compile

import (
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/viant/datly/repository/shape/plan"
)

var (
	useConnectorExpr = regexp.MustCompile(`(?i)use_connector\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*(?:'([a-zA-Z_][a-zA-Z0-9_]*)'|"([a-zA-Z_][a-zA-Z0-9_]*)"|([a-zA-Z_][a-zA-Z0-9_]*))\s*\)`)
	allowNullsExpr   = regexp.MustCompile(`(?i)allow_nulls\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)`)
	setLimitExpr     = regexp.MustCompile(`(?i)set_limit\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*(-?[0-9]+)\s*\)`)
)

type viewHint struct {
	Connector  string
	AllowNulls *bool
	NoLimit    *bool
}

func extractViewHints(dql string) map[string]viewHint {
	result := map[string]viewHint{}
	for _, match := range useConnectorExpr.FindAllStringSubmatch(dql, -1) {
		if len(match) < 5 {
			continue
		}
		alias := strings.TrimSpace(match[1])
		connector := strings.TrimSpace(match[2])
		if connector == "" {
			connector = strings.TrimSpace(match[3])
		}
		if connector == "" {
			connector = strings.TrimSpace(match[4])
		}
		if alias == "" || connector == "" {
			continue
		}
		hint := result[alias]
		hint.Connector = connector
		result[alias] = hint
	}
	for _, match := range allowNullsExpr.FindAllStringSubmatch(dql, -1) {
		if len(match) < 2 {
			continue
		}
		alias := strings.TrimSpace(match[1])
		if alias == "" {
			continue
		}
		hint := result[alias]
		value := true
		hint.AllowNulls = &value
		result[alias] = hint
	}
	for _, match := range setLimitExpr.FindAllStringSubmatch(dql, -1) {
		if len(match) < 3 {
			continue
		}
		alias := strings.TrimSpace(match[1])
		limitRaw := strings.TrimSpace(match[2])
		if alias == "" || limitRaw == "" {
			continue
		}
		limit, err := strconv.Atoi(limitRaw)
		if err != nil {
			continue
		}
		hint := result[alias]
		noLimit := limit == 0
		hint.NoLimit = &noLimit
		result[alias] = hint
	}
	return result
}

func appendRelationViews(result *plan.Result, root *plan.View, hints map[string]viewHint) {
	if result == nil || root == nil || len(root.Relations) == 0 {
		return
	}
	for _, relation := range root.Relations {
		if relation == nil {
			continue
		}
		name := strings.TrimSpace(relation.Ref)
		if name == "" {
			name = strings.TrimSpace(relation.Name)
		}
		if name == "" {
			continue
		}
		if len(relation.On) == 0 {
			continue
		}
		if _, exists := result.ViewsByName[name]; exists {
			continue
		}
		table := strings.TrimSpace(relation.Table)
		if table == "" {
			table = name
		}
		table = normalizeRelationTable(table)
		view := &plan.View{
			Path:        name,
			Holder:      name,
			Name:        name,
			Table:       table,
			Cardinality: "many",
			FieldType:   reflect.TypeOf([]map[string]interface{}{}),
			ElementType: reflect.TypeOf(map[string]interface{}{}),
		}
		result.Views = append(result.Views, view)
		result.ViewsByName[name] = view
	}
}

func applyViewHints(result *plan.Result, hints map[string]viewHint) {
	if result == nil || len(result.Views) == 0 {
		return
	}
	if len(hints) == 0 {
		return
	}
	for _, item := range result.Views {
		if item == nil {
			continue
		}
		for _, key := range []string{item.Name, item.Holder} {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			hint, ok := hints[key]
			if !ok {
				continue
			}
			if item.Connector == "" && hint.Connector != "" {
				item.Connector = hint.Connector
			}
			if item.AllowNulls == nil && hint.AllowNulls != nil {
				value := *hint.AllowNulls
				item.AllowNulls = &value
			}
			if item.SelectorNoLimit == nil && hint.NoLimit != nil {
				value := *hint.NoLimit
				item.SelectorNoLimit = &value
			}
		}
	}
}

func normalizeRelationTable(table string) string {
	table = strings.TrimSpace(table)
	if table == "" {
		return table
	}
	lower := strings.ToLower(table)
	fromIdx := strings.Index(lower, " from ")
	if fromIdx == -1 {
		return table
	}
	tail := strings.TrimSpace(table[fromIdx+6:])
	if tail == "" {
		return table
	}
	stop := len(tail)
	for i := 0; i < len(tail); i++ {
		switch tail[i] {
		case ' ', '\t', '\n', '\r', ')':
			stop = i
			i = len(tail)
		}
	}
	if stop == 0 {
		return table
	}
	normalized := strings.TrimSpace(tail[:stop])
	normalized = strings.Trim(normalized, "`\"")
	if normalized == "" {
		return table
	}
	return normalized
}
