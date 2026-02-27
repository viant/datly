package compile

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/repository/shape/plan"
)

type viewHint struct {
	Connector  string
	AllowNulls *bool
	NoLimit    *bool
}

func extractViewHints(dql string) map[string]viewHint {
	result := map[string]viewHint{}
	for _, call := range scanHintCalls(dql) {
		switch call.name {
		case "use_connector":
			if len(call.args) != 2 {
				continue
			}
			alias := strings.TrimSpace(call.args[0])
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
			alias := strings.TrimSpace(call.args[0])
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
			alias := strings.TrimSpace(call.args[0])
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
	result := make([]hintCall, 0)
	for i := 0; i < len(input); {
		if !isIdentifierStart(input[i]) {
			i++
			continue
		}
		start := i
		i++
		for i < len(input) && isIdentifierPart(input[i]) {
			i++
		}
		name := strings.ToLower(input[start:i])
		if name != "use_connector" && name != "allow_nulls" && name != "set_limit" {
			continue
		}
		j := skipSpaces(input, i)
		if j >= len(input) || input[j] != '(' {
			continue
		}
		body, end, ok := readCallBody(input, j)
		if !ok {
			continue
		}
		result = append(result, hintCall{name: name, args: splitCallArgs(body)})
		i = end + 1
	}
	return result
}

func readCallBody(input string, openParen int) (string, int, bool) {
	depth := 0
	quote := byte(0)
	for i := openParen; i < len(input); i++ {
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
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' {
			depth--
			if depth == 0 {
				return input[openParen+1 : i], i, true
			}
		}
	}
	return "", -1, false
}

func splitCallArgs(input string) []string {
	args := make([]string, 0)
	current := strings.Builder{}
	depth := 0
	quote := byte(0)
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if quote != 0 {
			current.WriteByte(ch)
			if ch == '\\' && i+1 < len(input) {
				i++
				current.WriteByte(input[i])
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			current.WriteByte(ch)
			continue
		}
		if ch == '(' {
			depth++
			current.WriteByte(ch)
			continue
		}
		if ch == ')' {
			if depth > 0 {
				depth--
			}
			current.WriteByte(ch)
			continue
		}
		if ch == ',' && depth == 0 {
			args = append(args, strings.TrimSpace(current.String()))
			current.Reset()
			continue
		}
		current.WriteByte(ch)
	}
	if value := strings.TrimSpace(current.String()); value != "" {
		args = append(args, value)
	}
	return args
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

func skipSpaces(input string, index int) int {
	for index < len(input) {
		switch input[index] {
		case ' ', '\t', '\n', '\r':
			index++
		default:
			return index
		}
	}
	return index
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
