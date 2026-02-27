package statement

import (
	"strings"

	"github.com/viant/sqlparser"
)

const (
	KindRead    = "read"
	KindExec    = "exec"
	KindService = "service"
)

type Statement struct {
	Start          int
	End            int
	Kind           string
	IsExec         bool
	SelectorMethod string
	Table          string
}

type Statements []*Statement

func (s Statements) IsExec() bool {
	if len(s) == 0 {
		return true
	}
	for _, item := range s {
		if item != nil && item.IsExec {
			return true
		}
	}
	return false
}

func (s Statements) DMLTables(rawSQL string) []string {
	var (
		tables = map[string]bool{}
		result []string
	)
	for _, statement := range s {
		if statement == nil || !statement.IsExec {
			continue
		}
		sqlText := slice(rawSQL, statement.Start, statement.End)
		if statement.Kind == KindService {
			if table := firstQuotedArgument(sqlText); table != "" {
				statement.Table = table
				if !tables[table] {
					result = append(result, table)
				}
				tables[table] = true
				continue
			}
		}
		lower := strings.ToLower(sqlText)
		switch {
		case strings.Contains(lower, "insert"):
			if parsed, _ := sqlparser.ParseInsert(sqlText); parsed != nil && parsed.Target.X != nil {
				statement.Table = strings.TrimSpace(sqlparser.Stringify(parsed.Target.X))
			}
		case strings.Contains(lower, "update"):
			if parsed, _ := sqlparser.ParseUpdate(sqlText); parsed != nil && parsed.Target.X != nil {
				statement.Table = strings.TrimSpace(sqlparser.Stringify(parsed.Target.X))
			}
		case strings.Contains(lower, "delete"):
			if parsed, _ := sqlparser.ParseDelete(sqlText); parsed != nil && parsed.Target.X != nil {
				statement.Table = strings.TrimSpace(sqlparser.Stringify(parsed.Target.X))
			}
		}
		if statement.Table == "" {
			continue
		}
		if !tables[statement.Table] {
			result = append(result, statement.Table)
		}
		tables[statement.Table] = true
	}
	return result
}

func New(sqlText string) Statements {
	if strings.TrimSpace(sqlText) == "" {
		return Statements{&Statement{Start: 0, End: 0}}
	}
	return parseStatements(sqlText)
}

func slice(input string, start, end int) string {
	if start < 0 {
		start = 0
	}
	if end < start {
		end = start
	}
	if end > len(input) {
		end = len(input)
	}
	return input[start:end]
}

func firstQuotedArgument(sqlText string) string {
	index := strings.Index(sqlText, `"`)
	if index == -1 {
		return ""
	}
	tail := sqlText[index+1:]
	end := strings.Index(tail, `"`)
	if end == -1 {
		return ""
	}
	return strings.TrimSpace(tail[:end])
}

func inferDefaultKind(sqlText string) (string, bool, string) {
	trimmed := strings.TrimSpace(strings.ToLower(sqlText))
	switch {
	case strings.HasPrefix(trimmed, "select"):
		return KindRead, false, ""
	case strings.HasPrefix(trimmed, "insert"),
		strings.HasPrefix(trimmed, "update"),
		strings.HasPrefix(trimmed, "delete"),
		strings.HasPrefix(trimmed, "call"),
		strings.HasPrefix(trimmed, "begin"):
		return KindExec, true, ""
	case strings.HasPrefix(trimmed, "$sql.insert"):
		return KindService, true, "Insert"
	case strings.HasPrefix(trimmed, "$sql.update"):
		return KindService, true, "Update"
	case strings.HasPrefix(trimmed, "$nop("):
		return KindExec, true, "Nop"
	default:
		return "", false, ""
	}
}
