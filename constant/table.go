package constant

import "strings"

// TableName identifies the existing full-table constant notation. Quoted SQL
// identifiers are literal names; values are resolved separately by the SQL owner.
func TableName(table string) string {
	value := strings.TrimSpace(table)
	if len(value) >= 2 && (value[0] == '`' && value[len(value)-1] == '`' || value[0] == '"' && value[len(value)-1] == '"') {
		return ""
	}
	switch {
	case strings.HasPrefix(value, "${Unsafe.") && strings.HasSuffix(value, "}"):
		return strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(value, "${Unsafe."), "}"))
	case strings.HasPrefix(value, "$Unsafe."):
		return strings.TrimSpace(strings.TrimPrefix(value, "$Unsafe."))
	case strings.HasPrefix(value, "Unsafe_"):
		return strings.TrimSpace(strings.TrimPrefix(value, "Unsafe_"))
	default:
		return ""
	}
}
