package discoverysql

import (
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

// PrepareDiscoverySQL rewrites SQL for zero-row schema discovery.
// It strips template constructs and injects 1 = 0 into every SELECT it can parse.
func PrepareDiscoverySQL(sql string) (string, bool) {
	cleaned := strings.TrimSpace(sql)
	if cleaned == "" {
		return cleaned, false
	}
	if hasTemplateVariables(cleaned) {
		cleaned = strings.TrimSpace(stripTemplateVariables(cleaned))
	}
	if cleaned == "" || !strings.Contains(strings.ToLower(cleaned), "select") {
		return cleaned, false
	}
	if rewritten, ok := falsifyQuery(cleaned); ok {
		return rewritten, true
	}
	return falsifyQueryText(cleaned)
}

func falsifyQuery(sql string) (string, bool) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return sql, false
	}
	parsed, err := sqlparser.ParseQuery(sql)
	if err != nil {
		return sql, false
	}
	originalShape := collectSelectShapes(parsed)
	falsifySelect(parsed)
	parsed.Limit = nil
	parsed.Offset = nil
	result, ok := safeStringify(parsed)
	if !ok || strings.TrimSpace(result) == "" {
		return sql, false
	}
	rewrittenParsed, err := sqlparser.ParseQuery(result)
	if err != nil {
		return sql, false
	}
	if !sameSelectShapes(originalShape, collectSelectShapes(rewrittenParsed)) {
		return sql, false
	}
	return result, true
}

func falsifySelect(sel *query.Select) {
	if sel == nil {
		return
	}
	injectFalsePredicate(sel)
	for _, ws := range sel.WithSelects {
		if ws != nil && ws.X != nil {
			falsifySelect(ws.X)
			ws.Raw = ""
		}
	}
	if sel.Union != nil && sel.Union.X != nil {
		falsifySelect(sel.Union.X)
	}
	falsifyFromSubquery(sel)
	for _, join := range sel.Joins {
		if join != nil {
			falsifyJoinSubquery(join)
		}
	}
}

func injectFalsePredicate(sel *query.Select) {
	if sel == nil {
		return
	}
	if containsFalsePredicate(sel.Qualify) {
		return
	}
	fp := &expr.Binary{
		X:  &expr.Literal{Value: "1"},
		Op: "=",
		Y:  &expr.Literal{Value: "0"},
	}
	if sel.Qualify == nil || sel.Qualify.X == nil {
		sel.Qualify = &expr.Qualify{X: fp}
		return
	}
	sel.Qualify = &expr.Qualify{
		X: &expr.Binary{
			X:  fp,
			Op: "AND",
			Y:  sel.Qualify.X,
		},
	}
}

func containsFalsePredicate(n node.Node) bool {
	switch actual := n.(type) {
	case nil:
		return false
	case *expr.Qualify:
		if actual == nil {
			return false
		}
		return containsFalsePredicate(actual.X)
	case *expr.Parenthesis:
		if actual == nil {
			return false
		}
		return containsFalsePredicate(actual.X)
	case *expr.Binary:
		if actual == nil {
			return false
		}
		if isFalseBinary(actual) {
			return true
		}
		return containsFalsePredicate(actual.X) || containsFalsePredicate(actual.Y)
	}
	return false
}

func isFalseBinary(b *expr.Binary) bool {
	if b == nil || strings.TrimSpace(b.Op) != "=" {
		return false
	}
	left := literalValue(b.X)
	right := literalValue(b.Y)
	return (left == "1" && right == "0") || (left == "0" && right == "1")
}

func literalValue(n node.Node) string {
	lit, ok := n.(*expr.Literal)
	if !ok || lit == nil {
		return ""
	}
	return strings.TrimSpace(lit.Value)
}

func falsifyFromSubquery(sel *query.Select) {
	if sel == nil || sel.From.X == nil {
		return
	}
	switch sub := sel.From.X.(type) {
	case *expr.Parenthesis:
		falsifySubqueryExpr(sub)
	case *expr.Raw:
		falsifyRawSubquery(sub)
	}
}

func falsifyRawSubquery(raw *expr.Raw) {
	if raw == nil {
		return
	}
	text := strings.TrimSpace(raw.Raw)
	if text == "" && raw.Unparsed != "" {
		text = strings.TrimSpace(raw.Unparsed)
	}
	if len(text) >= 2 && text[0] == '(' && text[len(text)-1] == ')' {
		text = text[1 : len(text)-1]
	}
	if !strings.Contains(strings.ToLower(text), "select") {
		return
	}
	subQuery, err := sqlparser.ParseQuery(text)
	if err != nil {
		return
	}
	falsifySelect(subQuery)
	if rewritten, ok := safeStringify(subQuery); ok {
		raw.Raw = "(" + rewritten + ")"
	}
}

func falsifyJoinSubquery(join *query.Join) {
	if join == nil || join.With == nil {
		return
	}
	if sub, ok := join.With.(*expr.Parenthesis); ok {
		falsifySubqueryExpr(sub)
	}
}

func falsifySubqueryExpr(paren *expr.Parenthesis) {
	if paren == nil || paren.X == nil {
		return
	}
	raw, ok := safeStringify(paren.X)
	if !ok {
		return
	}
	if !strings.Contains(strings.ToLower(strings.TrimSpace(raw)), "select") {
		return
	}
	subQuery, err := sqlparser.ParseQuery(raw)
	if err != nil {
		return
	}
	falsifySelect(subQuery)
	if rewritten, ok := safeStringify(subQuery); ok {
		paren.X = expr.NewRaw(rewritten)
	}
}

func safeStringify(n node.Node) (_ string, ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return sqlparser.Stringify(n), true
}

type selectShape struct {
	ListLen  int
	HasFrom  bool
	JoinLen  int
	WithLen  int
	HasUnion bool
}

func collectSelectShapes(sel *query.Select) []selectShape {
	if sel == nil {
		return nil
	}
	result := []selectShape{{
		ListLen:  len(sel.List),
		HasFrom:  sel.From.X != nil,
		JoinLen:  len(sel.Joins),
		WithLen:  len(sel.WithSelects),
		HasUnion: sel.Union != nil && sel.Union.X != nil,
	}}
	for _, ws := range sel.WithSelects {
		if ws != nil && ws.X != nil {
			result = append(result, collectSelectShapes(ws.X)...)
		}
	}
	if sel.Union != nil && sel.Union.X != nil {
		result = append(result, collectSelectShapes(sel.Union.X)...)
	}
	result = append(result, collectNestedShapes(sel.From.X)...)
	for _, join := range sel.Joins {
		if join != nil {
			result = append(result, collectNestedShapes(join.With)...)
		}
	}
	return result
}

func collectNestedShapes(n node.Node) []selectShape {
	switch actual := n.(type) {
	case nil:
		return nil
	case *expr.Parenthesis:
		if sub, ok := parseNestedSelect(actual.X); ok {
			return collectSelectShapes(sub)
		}
	case *expr.Raw:
		if sub, ok := parseNestedRawSelect(actual); ok {
			return collectSelectShapes(sub)
		}
	}
	return nil
}

func parseNestedSelect(n node.Node) (*query.Select, bool) {
	raw, ok := safeStringify(n)
	if !ok {
		return nil, false
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || !strings.Contains(strings.ToLower(raw), "select") {
		return nil, false
	}
	sub, err := sqlparser.ParseQuery(raw)
	if err != nil || sub == nil {
		return nil, false
	}
	return sub, true
}

func parseNestedRawSelect(raw *expr.Raw) (*query.Select, bool) {
	if raw == nil {
		return nil, false
	}
	text := strings.TrimSpace(raw.Raw)
	if text == "" && raw.Unparsed != "" {
		text = strings.TrimSpace(raw.Unparsed)
	}
	if len(text) >= 2 && text[0] == '(' && text[len(text)-1] == ')' {
		text = strings.TrimSpace(text[1 : len(text)-1])
	}
	if text == "" || !strings.Contains(strings.ToLower(text), "select") {
		return nil, false
	}
	sub, err := sqlparser.ParseQuery(text)
	if err != nil || sub == nil {
		return nil, false
	}
	return sub, true
}

func sameSelectShapes(a, b []selectShape) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func falsifyQueryText(sql string) (string, bool) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return sql, false
	}
	rewrittenNested, ok := rewriteNestedQueries(sql)
	if !ok {
		return sql, false
	}
	rewritten, ok := injectTopLevelFalsePredicate(rewrittenNested)
	if !ok {
		return sql, false
	}
	return rewritten, true
}

func rewriteNestedQueries(sql string) (string, bool) {
	var b strings.Builder
	changed := false
	for i := 0; i < len(sql); i++ {
		if sql[i] != '(' {
			b.WriteByte(sql[i])
			continue
		}
		end := matchParen(sql, i)
		if end == -1 {
			return sql, false
		}
		inner := sql[i+1 : end]
		trimmed := strings.TrimSpace(inner)
		if startsWithSelectQuery(trimmed) {
			if rewritten, ok := falsifyQueryText(trimmed); ok {
				b.WriteByte('(')
				b.WriteString(rewritten)
				b.WriteByte(')')
				changed = true
				i = end
				continue
			}
		}
		b.WriteString(sql[i : end+1])
		i = end
	}
	if !changed {
		return sql, true
	}
	return b.String(), true
}

func injectTopLevelFalsePredicate(sql string) (string, bool) {
	fromPos := findTopLevelKeyword(sql, "from")
	wherePos := findTopLevelKeyword(sql, "where")
	groupPos := findTopLevelKeyword(sql, "group by")
	havingPos := findTopLevelKeyword(sql, "having")
	qualifyPos := findTopLevelKeyword(sql, "qualify")
	orderPos := findTopLevelKeyword(sql, "order by")
	limitPos := findTopLevelKeyword(sql, "limit")
	unionPos := findTopLevelKeyword(sql, "union")

	if fromPos == -1 && wherePos == -1 {
		return sql, true
	}

	if wherePos != -1 {
		endWhere := firstPositive(groupPos, havingPos, qualifyPos, orderPos, limitPos, unionPos, len(sql))
		whereClause := sql[wherePos:endWhere]
		if containsFalsePredicateText(whereClause) {
			return sql, true
		}
		return injectWithSpacing(sql, endWhere, " AND 1 = 0"), true
	}

	insertPos := firstPositive(groupPos, havingPos, qualifyPos, orderPos, limitPos, unionPos, len(sql))
	if insertPos < 0 {
		insertPos = len(sql)
	}
	return injectWithSpacing(sql, insertPos, " WHERE 1 = 0"), true
}

func injectWithSpacing(sql string, pos int, injection string) string {
	left := sql[:pos]
	right := sql[pos:]
	if len(left) > 0 {
		last := left[len(left)-1]
		if last != ' ' && last != '\n' && last != '\t' && last != '\r' {
			injection = " " + strings.TrimLeft(injection, " ")
		}
	}
	if len(right) > 0 {
		first := right[0]
		if first != ' ' && first != '\n' && first != '\t' && first != '\r' {
			injection += " "
		}
	}
	return left + injection + right
}

func containsFalsePredicateText(sql string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(sql), " "))
	return strings.Contains(normalized, "1 = 0") || strings.Contains(normalized, "0 = 1")
}

func startsWithSelectQuery(sql string) bool {
	lower := strings.ToLower(strings.TrimSpace(sql))
	return strings.HasPrefix(lower, "select") || strings.HasPrefix(lower, "with")
}

func findTopLevelKeyword(sql, keyword string) int {
	lower := strings.ToLower(sql)
	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(lower); i++ {
		ch := lower[i]
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
		}
		if inSingle || inDouble || inBacktick {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && hasKeywordAt(lower, i, keyword) {
			return i
		}
	}
	return -1
}

func firstPositive(values ...int) int {
	best := -1
	for _, value := range values {
		if value < 0 {
			continue
		}
		if best == -1 || value < best {
			best = value
		}
	}
	return best
}

func hasKeywordAt(text string, pos int, keyword string) bool {
	if pos < 0 || pos+len(keyword) > len(text) || text[pos:pos+len(keyword)] != keyword {
		return false
	}
	beforeOK := pos == 0 || !isKeywordIdentChar(text[pos-1])
	afterPos := pos + len(keyword)
	afterOK := afterPos == len(text) || !isKeywordIdentChar(text[afterPos])
	return beforeOK && afterOK
}

func isKeywordIdentChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '$'
}

func matchParen(sql string, start int) int {
	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := start; i < len(sql); i++ {
		ch := sql[i]
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
		}
		if inSingle || inDouble || inBacktick {
			continue
		}
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func hasTemplateVariables(sql string) bool {
	for i := 0; i < len(sql)-1; i++ {
		if sql[i] == '$' && isIdentStart(sql[i+1]) {
			return true
		}
		if sql[i] == '#' && (sql[i+1] == 'i' || sql[i+1] == 'f' || sql[i+1] == 'e' || sql[i+1] == 's') {
			return true
		}
		if sql[i] == '$' && sql[i+1] == '{' {
			return true
		}
	}
	return false
}

func stripTemplateVariables(sql string) string {
	var b strings.Builder
	b.Grow(len(sql))
	i := 0
	for i < len(sql) {
		if sql[i] == '#' && i+1 < len(sql) {
			directive := matchDirective(sql, i)
			if directive != "" {
				end := skipDirective(sql, i, directive)
				b.WriteByte(' ')
				i = end
				continue
			}
		}
		if sql[i] == '$' && i+1 < len(sql) {
			next := sql[i+1]
			if next == '{' {
				depth := 1
				j := i + 2
				for j < len(sql) && depth > 0 {
					if sql[j] == '{' {
						depth++
					} else if sql[j] == '}' {
						depth--
					}
					j++
				}
				b.WriteString("''")
				i = j
				continue
			}
			if isIdentStart(next) {
				j := i + 1
				for j < len(sql) && isIdentPart(sql[j]) {
					j++
				}
				hasMethodCall := false
				methodExpr := ""
				for j < len(sql) && sql[j] == '.' {
					methodStart := j
					j++
					for j < len(sql) && isIdentPart(sql[j]) {
						j++
					}
					if j < len(sql) && sql[j] == '(' {
						hasMethodCall = true
						methodExpr = sql[methodStart:j]
						depth := 1
						j++
						for j < len(sql) && depth > 0 {
							if sql[j] == '(' {
								depth++
							} else if sql[j] == ')' {
								depth--
							}
							j++
						}
					}
				}
				if hasMethodCall {
					if strings.EqualFold(methodExpr, ".AppendBinding") {
						b.WriteString("''")
					}
				} else {
					b.WriteString("''")
				}
				i = j
				continue
			}
		}
		b.WriteByte(sql[i])
		i++
	}
	return b.String()
}

func matchDirective(sql string, pos int) string {
	directives := []string{"#foreach", "#if", "#elseif", "#else", "#end", "#set", "#settings", "#setting", "#define", "#package", "#import"}
	remaining := sql[pos:]
	for _, d := range directives {
		if len(remaining) >= len(d) && strings.EqualFold(remaining[:len(d)], d) {
			if len(remaining) == len(d) || !isIdentPart(remaining[len(d)]) {
				return d
			}
		}
	}
	return ""
}

func skipDirective(sql string, pos int, directive string) int {
	switch {
	case directive == "#set" || directive == "#settings" || directive == "#setting" || directive == "#define":
		j := pos + len(directive)
		for j < len(sql) && (sql[j] == ' ' || sql[j] == '\t') {
			j++
		}
		if j < len(sql) && sql[j] == '(' {
			depth := 1
			j++
			for j < len(sql) && depth > 0 {
				if sql[j] == '(' {
					depth++
				} else if sql[j] == ')' {
					depth--
				}
				j++
			}
			return j
		}
		for j < len(sql) && sql[j] != '\n' {
			j++
		}
		if j < len(sql) {
			j++
		}
		return j
	case directive == "#foreach" || directive == "#if":
		j := pos + len(directive)
		depth := 1
		for j < len(sql) && depth > 0 {
			d := matchDirective(sql, j)
			if d == "#if" || d == "#foreach" {
				depth++
				j += len(d)
			} else if d == "#end" {
				depth--
				j += len(d)
			} else {
				j++
			}
		}
		return j
	default:
		j := pos + len(directive)
		for j < len(sql) && sql[j] != '\n' {
			j++
		}
		if j < len(sql) {
			j++
		}
		return j
	}
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentPart(ch byte) bool {
	return isIdentStart(ch) || (ch >= '0' && ch <= '9')
}
