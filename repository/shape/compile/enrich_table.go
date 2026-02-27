package compile

// enrich_table.go — table-name inference logic extracted from enrich.go.
// All functions here derive a database table name from SQL text, file-system
// sibling files, or embedded SQL references.

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/compile/pipeline"
	"github.com/viant/datly/repository/shape/plan"
)

func shouldInferTable(item *plan.View) bool {
	if item == nil {
		return false
	}
	name := strings.TrimSpace(item.Name)
	table := strings.TrimSpace(item.Table)
	if table == "" {
		return true
	}
	if strings.HasPrefix(table, "(") {
		return true
	}
	if normalizedTemplatePlaceholderTable(table) {
		return true
	}
	return strings.EqualFold(name, table)
}

func normalizedTemplatePlaceholderTable(table string) bool {
	if table == "" {
		return false
	}
	parts := strings.Split(table, ".")
	if len(parts) < 3 {
		return false
	}
	for i := 0; i < len(parts)-1; i++ {
		part := strings.TrimSpace(parts[i])
		if part == "" {
			return false
		}
		for _, ch := range part {
			if ch < '0' || ch > '9' {
				return false
			}
		}
	}
	return true
}

func inferTableFromSQL(sqlText string, source *shape.Source) string {
	sqlText = strings.TrimSpace(sqlText)
	if sqlText == "" {
		return ""
	}
	if expr := topLevelFromExpr(sqlText); expr != "" {
		if table := tableFromFromExpr(expr, source); table != "" {
			return table
		}
	}
	if table := pipeline.InferTableFromSQL(sqlText); table != "" {
		if !strings.EqualFold(table, "DQLView") {
			return table
		}
	}
	if table := inferFromEmbeddedSQL(sqlText, source); table != "" {
		return table
	}
	return ""
}

func inferFromEmbeddedSQL(sqlText string, source *shape.Source) string {
	ref, ok := findFirstEmbedRef(sqlText)
	if !ok {
		return ""
	}
	ref = strings.Trim(ref, `"'`)
	if ref == "" {
		return ""
	}
	resolved := resolveEmbedPath(source, ref)
	if resolved == "" {
		return ""
	}
	embedded, err := os.ReadFile(resolved)
	if err != nil {
		return ""
	}
	queryNode, _, err := pipeline.ParseSelectWithDiagnostic(string(embedded))
	if err != nil || queryNode == nil {
		if table := pipeline.InferTableFromSQL(string(embedded)); table != "" && !strings.EqualFold(table, "DQLView") {
			return strings.Trim(table, "`\"")
		}
		return ""
	}
	_, table, err := pipeline.InferRoot(queryNode, "")
	if err != nil || strings.TrimSpace(table) == "" {
		return ""
	}
	if strings.EqualFold(strings.TrimSpace(table), "DQLView") {
		return ""
	}
	return strings.Trim(table, "`\"")
}

func resolveEmbedPath(source *shape.Source, ref string) string {
	if filepath.IsAbs(ref) {
		return ref
	}
	if source == nil || strings.TrimSpace(source.Path) == "" {
		return ""
	}
	base := source.Path
	if fi, err := os.Stat(base); err == nil && fi.IsDir() {
		return filepath.Clean(filepath.Join(base, ref))
	}
	return filepath.Clean(filepath.Join(filepath.Dir(base), ref))
}

func inferTableFromSiblingSQL(viewName string, source *shape.Source) string {
	viewName = strings.TrimSpace(viewName)
	if viewName == "" || source == nil || strings.TrimSpace(source.Path) == "" {
		return ""
	}
	sibling := filepath.Join(filepath.Dir(source.Path), viewName+".sql")
	data, err := os.ReadFile(sibling)
	if err != nil {
		sibling = filepath.Join(filepath.Dir(source.Path), strings.ToLower(viewName)+".sql")
		data, err = os.ReadFile(sibling)
	}
	if err != nil {
		return ""
	}
	return inferTableFromSQL(string(data), source)
}

func inferTableFromEmbedRef(source *shape.Source, ref string) string {
	ref = strings.Trim(strings.TrimSpace(ref), `"'`)
	if ref == "" {
		return ""
	}
	resolved := resolveEmbedPath(source, ref)
	if resolved == "" {
		return ""
	}
	data, err := os.ReadFile(resolved)
	if err != nil {
		return ""
	}
	return pipeline.InferTableFromSQL(string(data))
}

// topLevelFromExpr scans sqlText for the first top-level (depth-0) FROM keyword
// and returns the expression that immediately follows it, including subquery parens
// with a trailing alias when present.
func topLevelFromExpr(sqlText string) string {
	lower := strings.ToLower(sqlText)
	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(sqlText); i++ {
		ch := sqlText[i]
		switch ch {
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
		case '(':
			if !inSingle && !inDouble && !inBacktick {
				depth++
			}
		case ')':
			if !inSingle && !inDouble && !inBacktick && depth > 0 {
				depth--
			}
		}
		if inSingle || inDouble || inBacktick || depth != 0 {
			continue
		}
		if i+6 > len(sqlText) {
			break
		}
		if lower[i:i+4] != "from" {
			continue
		}
		if i > 0 {
			prev := lower[i-1]
			if (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9') || prev == '_' {
				continue
			}
		}
		j := i + 4
		for j < len(sqlText) && (sqlText[j] == ' ' || sqlText[j] == '\n' || sqlText[j] == '\t' || sqlText[j] == '\r') {
			j++
		}
		if j >= len(sqlText) {
			return ""
		}
		if sqlText[j] == '(' {
			start := j
			d := 0
			for ; j < len(sqlText); j++ {
				if sqlText[j] == '(' {
					d++
				} else if sqlText[j] == ')' {
					d--
					if d == 0 {
						j++
						break
					}
				}
			}
			for j < len(sqlText) && (sqlText[j] == ' ' || sqlText[j] == '\n' || sqlText[j] == '\t' || sqlText[j] == '\r') {
				j++
			}
			for j < len(sqlText) {
				c := sqlText[j]
				if !(c == '_' || c == '.' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
					break
				}
				j++
			}
			return strings.TrimSpace(sqlText[start:j])
		}
		start := j
		for j < len(sqlText) {
			c := sqlText[j]
			if !(c == '_' || c == '.' || c == '/' || c == '{' || c == '}' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '$') {
				break
			}
			j++
		}
		return strings.TrimSpace(sqlText[start:j])
	}
	return ""
}

func tableFromFromExpr(fromExpr string, source *shape.Source) string {
	fromExpr = strings.TrimSpace(fromExpr)
	if fromExpr == "" {
		return ""
	}
	if strings.HasPrefix(fromExpr, "(") {
		if table := inferFromEmbeddedSQL(fromExpr, source); table != "" {
			return table
		}
		inner := fromExpr
		if idx := strings.LastIndex(inner, ")"); idx > 0 {
			inner = strings.TrimSpace(inner[1:idx])
		}
		return inferTableFromSQL(inner, source)
	}
	return strings.Trim(fromExpr, "`\"")
}

func inferConnector(item *plan.View, source *shape.Source) string {
	if item == nil {
		return ""
	}
	path := ""
	if source != nil {
		path = strings.ToLower(strings.ReplaceAll(source.Path, "\\", "/"))
	}
	table := strings.ToUpper(strings.TrimSpace(item.Table))
	switch {
	case strings.Contains(path, "/dql/system/"):
		return "system"
	case strings.HasPrefix(table, "CI_") || strings.Contains(table, ".CI_"):
		return "ci_ads"
	case strings.Contains(path, "/dql/ui/"):
		return "sitemgmt"
	case strings.Contains(table, "SITE"):
		return "sitemgmt"
	default:
		return ""
	}
}

func normalizeRootViewName(result *plan.Result, sourceName string) {
	if result == nil || len(result.Views) == 0 {
		return
	}
	root := result.Views[0]
	if root == nil {
		return
	}
	desired := sourceName
	if desired == "" {
		return
	}
	current := strings.TrimSpace(root.Name)
	if current == "" {
		root.Name = desired
		root.Path = desired
		root.Holder = desired
		return
	}
	if strings.EqualFold(current, desired) {
		return
	}
	suspicious := map[string]bool{
		"and": true, "or": true, "status": true, "value": true, "watching": true,
	}
	if !suspicious[strings.ToLower(current)] {
		return
	}
	if result.ViewsByName != nil {
		delete(result.ViewsByName, root.Name)
	} else {
		result.ViewsByName = map[string]*plan.View{}
	}
	root.Name = desired
	root.Path = desired
	root.Holder = desired
	result.ViewsByName[root.Name] = root
}
