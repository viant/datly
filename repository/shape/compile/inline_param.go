package compile

import (
	"encoding/json"
	"strings"

	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/view/state"
)

type inlineParamHint struct {
	Kind     string `json:"Kind"`
	Location string `json:"Location"`
	DataType string `json:"DataType"`
	Required *bool  `json:"Required"`
}

// applyInlineParamHints scans the SQL text for patterns like
// $varName /* {"Kind":"header","Location":"Header-Name"} */ and updates
// matching state parameters in the plan result.
func applyInlineParamHints(sqlText string, result *plan.Result) {
	if result == nil || strings.TrimSpace(sqlText) == "" {
		return
	}
	hints := extractInlineParamHints(sqlText)
	if len(hints) == 0 {
		return
	}
	for _, st := range result.States {
		if st == nil {
			continue
		}
		name := strings.TrimPrefix(strings.TrimSpace(st.Name), "$")
		hint, ok := hints[name]
		if !ok {
			continue
		}
		if hint.Kind != "" && st.In != nil {
			st.In.Kind = state.Kind(strings.ToLower(hint.Kind))
		}
		if hint.Location != "" && st.In != nil {
			st.In.Name = hint.Location
		}
		if hint.DataType != "" {
			ensureStateSchema(st).DataType = hint.DataType
		}
		if hint.Required != nil {
			st.Required = hint.Required
		}
	}
}

func extractInlineParamHints(sql string) map[string]inlineParamHint {
	result := map[string]inlineParamHint{}
	i := 0
	for i < len(sql) {
		if sql[i] != '$' {
			i++
			continue
		}
		i++
		start := i
		for i < len(sql) && isParamIdentPart(sql[i]) {
			i++
		}
		if i == start {
			continue
		}
		name := sql[start:i]
		j := skipInlineSpaces(sql, i)
		if j+1 >= len(sql) || sql[j] != '/' || sql[j+1] != '*' {
			continue
		}
		endComment := strings.Index(sql[j+2:], "*/")
		if endComment < 0 {
			continue
		}
		body := strings.TrimSpace(sql[j+2 : j+2+endComment])
		if !strings.HasPrefix(body, "{") || !strings.HasSuffix(body, "}") {
			continue
		}
		var hint inlineParamHint
		if err := json.Unmarshal([]byte(body), &hint); err != nil {
			continue
		}
		if hint.Kind != "" || hint.Location != "" || hint.DataType != "" {
			result[name] = hint
		}
		i = j + 2 + endComment + 2
	}
	return result
}

func isParamIdentPart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_'
}

func skipInlineSpaces(input string, index int) int {
	for index < len(input) && (input[index] == ' ' || input[index] == '\t' || input[index] == '\n' || input[index] == '\r') {
		index++
	}
	return index
}
