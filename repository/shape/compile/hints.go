package compile

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/repository/shape/dql/decl"
	"github.com/viant/datly/repository/shape/plan"
)

type viewHint struct {
	Connector   string
	AllowNulls  *bool
	NoLimit     *bool
	CacheRef    string
	Limit       *int
	Cardinality string
	Dest        string
	TypeName    string
	Self        *plan.SelfReference
}

func extractViewHints(dql string) map[string]viewHint {
	result := map[string]viewHint{}
	for _, call := range scanHintCalls(dql) {
		switch call.name {
		case "use_connector":
			if len(call.args) != 2 {
				continue
			}
			alias := normalizeHintAlias(call.args[0])
			connector := unquote(strings.TrimSpace(call.args[1]))
			if !isIdentifier(alias) || !isIdentifier(connector) {
				continue
			}
			hint := result[alias]
			hint.Connector = connector
			result[alias] = hint
		case "allow_nulls":
			if len(call.args) != 1 {
				continue
			}
			alias := normalizeHintAlias(call.args[0])
			if !isIdentifier(alias) {
				continue
			}
			hint := result[alias]
			value := true
			hint.AllowNulls = &value
			result[alias] = hint
		case "set_limit":
			if len(call.args) != 2 {
				continue
			}
			alias := normalizeHintAlias(call.args[0])
			limitRaw := strings.TrimSpace(call.args[1])
			if !isIdentifier(alias) || limitRaw == "" {
				continue
			}
			limit, err := strconv.Atoi(limitRaw)
			if err != nil {
				continue
			}
			hint := result[alias]
			noLimit := limit == 0
			hint.NoLimit = &noLimit
			if limit > 0 {
				hint.Limit = &limit
			}
			result[alias] = hint
		case "set_cache":
			if len(call.args) != 2 {
				continue
			}
			alias := normalizeHintAlias(call.args[0])
			ref := unquote(strings.TrimSpace(call.args[1]))
			if !isIdentifier(alias) || ref == "" {
				continue
			}
			hint := result[alias]
			hint.CacheRef = ref
			result[alias] = hint
		case "cardinality":
			if len(call.args) != 2 {
				continue
			}
			alias := normalizeHintAlias(call.args[0])
			value := strings.ToLower(strings.TrimSpace(unquote(strings.TrimSpace(call.args[1]))))
			if !isIdentifier(alias) {
				continue
			}
			if value != "one" && value != "many" {
				continue
			}
			hint := result[alias]
			hint.Cardinality = value
			result[alias] = hint
		case "self_ref":
			if len(call.args) != 4 {
				continue
			}
			alias := normalizeHintAlias(call.args[0])
			holder := unquote(strings.TrimSpace(call.args[1]))
			child := unquote(strings.TrimSpace(call.args[2]))
			parent := unquote(strings.TrimSpace(call.args[3]))
			if alias == "" || holder == "" || child == "" || parent == "" {
				continue
			}
			hint := result[alias]
			hint.Self = &plan.SelfReference{Holder: holder, Child: child, Parent: parent}
			result[alias] = hint
		case "dest":
			if len(call.args) != 2 {
				continue
			}
			alias := normalizeHintAlias(call.args[0])
			dest := strings.TrimSpace(unquote(strings.TrimSpace(call.args[1])))
			if !isIdentifier(alias) || dest == "" {
				continue
			}
			hint := result[alias]
			hint.Dest = dest
			result[alias] = hint
		case "type":
			if len(call.args) != 2 {
				continue
			}
			alias := normalizeHintAlias(call.args[0])
			typeName := strings.TrimSpace(unquote(strings.TrimSpace(call.args[1])))
			if !isIdentifier(alias) || typeName == "" {
				continue
			}
			hint := result[alias]
			hint.TypeName = typeName
			result[alias] = hint
		}
	}
	return result
}

type hintCall struct {
	name string
	args []string
}

func scanHintCalls(input string) []hintCall {
	names := map[string]bool{
		"use_connector": true,
		"allow_nulls":   true,
		"set_limit":     true,
		"set_cache":     true,
		"cardinality":   true,
		"self_ref":      true,
		"dest":          true,
		"type":          true,
	}
	parsed, _ := decl.ScanCalls(input, decl.CallScanOptions{
		AllowedNames:  names,
		RequireDollar: false,
		AllowDollar:   false,
		Strict:        false,
	})
	result := make([]hintCall, 0, len(parsed))
	for _, call := range parsed {
		result = append(result, hintCall{name: call.Name, args: call.Args})
	}
	return result
}

func isIdentifierStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentifierPart(ch byte) bool {
	return isIdentifierStart(ch) || (ch >= '0' && ch <= '9')
}

func isIdentifier(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || !isIdentifierStart(value[0]) {
		return false
	}
	for i := 1; i < len(value); i++ {
		if !isIdentifierPart(value[i]) {
			return false
		}
	}
	return true
}

func unquote(value string) string {
	if len(value) >= 2 {
		first := value[0]
		last := value[len(value)-1]
		if (first == '\'' && last == '\'') || (first == '"' && last == '"') {
			return value[1 : len(value)-1]
		}
	}
	return value
}

func appendRelationViews(result *plan.Result, root *plan.View, hints map[string]viewHint, rawDQL string) {
	if result == nil || root == nil || len(root.Relations) == 0 {
		return
	}
	joinSQLByAlias := map[string]string{}
	for _, item := range scanJoinSubqueries(rawDQL) {
		alias := strings.TrimSpace(item.alias)
		body := strings.TrimSpace(item.body)
		if alias == "" || body == "" {
			continue
		}
		joinSQLByAlias[strings.ToLower(alias)] = body
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
		sqlText := strings.TrimSpace(joinSQLByAlias[strings.ToLower(name)])
		if sqlText == "" {
			sqlText = relationSQLText(table)
		}
		if table == "" {
			table = name
		}
		table = normalizeRelationTable(table)
		view := &plan.View{
			Path:        name,
			Holder:      name,
			Name:        name,
			Table:       table,
			SQL:         sqlText,
			Cardinality: "many",
			FieldType:   reflect.TypeOf([]map[string]interface{}{}),
			ElementType: reflect.TypeOf(map[string]interface{}{}),
		}
		if len(relation.ColumnsConfig) > 0 {
			view.Declaration = &plan.ViewDeclaration{ColumnsConfig: relation.ColumnsConfig}
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
			hint, ok := lookupViewHint(hints, key)
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
			if item.SelectorLimit == nil && hint.Limit != nil {
				value := *hint.Limit
				item.SelectorLimit = &value
			}
			if item.CacheRef == "" && hint.CacheRef != "" {
				item.CacheRef = hint.CacheRef
			}
			if hint.Cardinality != "" {
				item.Cardinality = hint.Cardinality
			}
			if item.Self == nil && hint.Self != nil {
				item.Self = hint.Self
			}
			if hint.Dest != "" || hint.TypeName != "" {
				if item.Declaration == nil {
					item.Declaration = &plan.ViewDeclaration{}
				}
				if item.Declaration.Dest == "" && hint.Dest != "" {
					item.Declaration.Dest = hint.Dest
				}
				if item.Declaration.TypeName == "" && hint.TypeName != "" {
					item.Declaration.TypeName = hint.TypeName
				}
			}
		}
	}
}

func normalizeHintAlias(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func lookupViewHint(hints map[string]viewHint, key string) (viewHint, bool) {
	key = normalizeHintAlias(key)
	if key == "" {
		return viewHint{}, false
	}
	hint, ok := hints[key]
	return hint, ok
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

func relationSQLText(table string) string {
	trimmed := strings.TrimSpace(table)
	if trimmed == "" {
		return ""
	}
	normalized := strings.ToLower(trimmed)
	if strings.HasPrefix(normalized, "select ") {
		return trimmed
	}
	if strings.HasPrefix(trimmed, "(") {
		unwrapped := unwrapRelationParens(trimmed)
		unwrappedLower := strings.ToLower(strings.TrimSpace(unwrapped))
		if strings.HasPrefix(unwrappedLower, "select ") {
			return strings.TrimSpace(unwrapped)
		}
	}
	return ""
}

func unwrapRelationParens(input string) string {
	input = strings.TrimSpace(input)
	if len(input) < 2 || input[0] != '(' || input[len(input)-1] != ')' {
		return input
	}
	depth := 0
	quote := byte(0)
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if quote != 0 {
			if ch == '\\' && i+1 < len(input) {
				i++
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(input)-1 {
				return input
			}
		}
	}
	if depth != 0 {
		return input
	}
	inner := strings.TrimSpace(input[1 : len(input)-1])
	if inner == "" {
		return input
	}
	return inner
}
