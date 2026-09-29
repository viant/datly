package column

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/view"
	viewcolumn "github.com/viant/datly/view/column"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlx/io"
)

// Detector resolves columns for shape-generated views.
//
// Rules:
//   - schema field order is canonical order
//   - wildcard SQL always performs DB discovery
//   - newly discovered columns are appended at the end
//   - matched columns keep schema order but refresh metadata from DB
type Detector struct{}

func New() *Detector {
	return &Detector{}
}

func (d *Detector) Resolve(ctx context.Context, resource *view.Resource, aView *view.View) (view.Columns, error) {
	if aView == nil {
		return nil, fmt.Errorf("shape column detector: nil view")
	}

	base := columnsFromSchema(aView)
	// If columns are placeholders (col_1, col_2, etc.) from static inference, treat as no columns
	if allPlaceholderColumns(aView.Columns) {
		base = nil
	}
	if explicit := explicitProjectedSubqueryColumns(aView); len(explicit) > 0 {
		if len(base) == 0 {
			return explicit, nil
		}
		return mergePreservingOrder(base, explicit), nil
	}
	if !needsDiscovery(aView) && len(base) > 0 {
		return base, nil
	}

	discovered, err := d.detect(ctx, resource, aView)
	if err != nil {
		return nil, err
	}
	if len(base) == 0 {
		return discovered, nil
	}
	return mergePreservingOrder(base, discovered), nil
}

func (d *Detector) detect(ctx context.Context, resource *view.Resource, aView *view.View) (view.Columns, error) {
	connector, err := lookupConnector(ctx, resource, aView)
	if err != nil {
		return nil, err
	}
	db, err := connector.DB()
	if err != nil {
		return nil, fmt.Errorf("shape column detector: failed to open db for view %s: %w", aView.Name, err)
	}
	query := discoverySQL(aView, resource)
	table := resolveDiscoveryTable(aView, resource, sourceSQL(aView))
	sqlColumns, err := viewcolumn.Discover(ctx, db, table, query)
	if err != nil {
		return nil, fmt.Errorf("shape column detector: discover failed for view %s (query=%q, table=%q): %w", aView.Name, query, table, err)
	}
	return view.NewColumns(sqlColumns, aView.ColumnsConfig), nil
}

// discoverySQL returns SQL suitable for column discovery.
// Strategy:
//  1. Strip template variables ($var, #if...#end, ${expr})
//  2. Inject 1=0 into every SELECT in the query (CTEs, UNIONs, subqueries)
//     This ensures zero rows scanned — safe for BigQuery (no full scan cost)
//  3. Fall back to table name if parsing/falsification fails
func discoverySQL(aView *view.View, resource *view.Resource) string {
	raw := sourceSQL(aView)
	if expanded := applyConstValuesForDiscovery(raw, resource); strings.TrimSpace(expanded) != "" {
		raw = expanded
	}
	table := resolveDiscoveryTable(aView, resource, raw)
	if raw == "" {
		return table
	}
	// EXCEPT clause is a datly projection extension; table fallback is safest.
	if table != "" && hasExceptClause(raw) {
		return table
	}
	// Template SQL with wildcard fallback to table metadata. For explicit projection
	// we still derive columns from SQL (after template stripping) to avoid widening
	// contract to the whole table.
	if table != "" && hasTemplateVariables(raw) && usesWildcard(aView) {
		return table
	}
	// For clean SQL without templates, try to falsify for column type inference
	cleaned := strings.TrimSpace(raw)
	if hasTemplateVariables(cleaned) {
		cleaned = strings.TrimSpace(stripTemplateVariables(cleaned))
	}
	if cleaned == "" || !strings.Contains(strings.ToLower(cleaned), "select") {
		if table != "" {
			return table
		}
		return cleaned
	}
	if falsified, ok := falsifyQuery(cleaned); ok {
		return falsified
	}
	// Fallback to table
	if table != "" {
		return table
	}
	return cleaned
}

func explicitProjectedSubqueryColumns(aView *view.View) view.Columns {
	if aView == nil || !usesWildcard(aView) {
		return nil
	}
	sql := strings.TrimSpace(sourceSQL(aView))
	if sql == "" {
		return nil
	}
	queryNode, err := sqlparser.ParseQuery(sql)
	if err != nil || queryNode == nil || !queryNode.List.IsStarExpr() || queryNode.From.X == nil {
		return nil
	}
	fromExpr := strings.TrimSpace(sqlparser.Stringify(queryNode.From.X))
	if fromExpr == "" {
		return nil
	}
	fromExpr = strings.TrimSpace(strings.TrimPrefix(fromExpr, "("))
	fromExpr = strings.TrimSpace(strings.TrimSuffix(fromExpr, ")"))
	if !strings.Contains(strings.ToLower(fromExpr), "select") {
		return nil
	}
	innerQuery, err := sqlparser.ParseQuery(fromExpr)
	if err != nil || innerQuery == nil {
		return nil
	}
	columns := sqlparser.NewColumns(innerQuery.List)
	if len(columns) == 0 || columns.IsStarExpr() {
		return nil
	}
	normalizeExplicitProjectedColumnTypes(columns)
	return view.NewColumns(columns, aView.ColumnsConfig)
}

func normalizeExplicitProjectedColumnTypes(columns sqlparser.Columns) {
	for _, column := range columns {
		if column == nil || strings.TrimSpace(column.Type) != "" {
			continue
		}
		expression := strings.TrimSpace(column.Expression)
		trimmed := strings.TrimSpace(strings.Trim(expression, "()"))
		switch {
		case trimmed == "":
			continue
		case trimmed == "true" || trimmed == "false":
			column.Type = "bool"
		case isIntegerLiteral(trimmed):
			column.Type = "int"
		case isFloatLiteral(trimmed):
			column.Type = "float64"
		case isQuotedLiteral(trimmed):
			column.Type = "string"
		}
	}
}

func isIntegerLiteral(value string) bool {
	if value == "" {
		return false
	}
	for i, ch := range value {
		if i == 0 && (ch == '-' || ch == '+') {
			if len(value) == 1 {
				return false
			}
			continue
		}
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func isFloatLiteral(value string) bool {
	if value == "" || strings.Count(value, ".") != 1 {
		return false
	}
	value = strings.ReplaceAll(value, ".", "")
	return isIntegerLiteral(value)
}

func isQuotedLiteral(value string) bool {
	return len(value) >= 2 && ((value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"'))
}

func resolveDiscoveryTable(aView *view.View, resource *view.Resource, rawSQL string) string {
	table := ""
	if aView != nil {
		table = strings.TrimSpace(aView.Table)
	}
	if expanded := strings.TrimSpace(applyConstValuesForDiscovery(table, resource)); expanded != "" {
		table = expanded
	}
	table = normalizeDiscoveryTable(table)
	if table == "" {
		table = inferDiscoveryTable(rawSQL)
	}
	return table
}

func applyConstValuesForDiscovery(sql string, resource *view.Resource) string {
	if strings.TrimSpace(sql) == "" || resource == nil || len(resource.Parameters) == 0 {
		return sql
	}
	consts := map[string]string{}
	for _, item := range resource.Parameters {
		if item == nil || item.In == nil || item.In.Kind != state.KindConst {
			continue
		}
		name := strings.TrimSpace(item.Name)
		if name == "" {
			name = strings.TrimSpace(item.In.Name)
		}
		if name == "" || item.Value == nil {
			continue
		}
		consts[name] = fmt.Sprintf("%v", item.Value)
	}
	if len(consts) == 0 {
		return sql
	}
	var b strings.Builder
	b.Grow(len(sql))
	for i := 0; i < len(sql); {
		if sql[i] != '$' {
			b.WriteByte(sql[i])
			i++
			continue
		}
		if i+1 < len(sql) && sql[i+1] == '{' {
			end := i + 2
			for end < len(sql) && sql[end] != '}' {
				end++
			}
			if end >= len(sql) {
				b.WriteString(sql[i:])
				break
			}
			expr := strings.TrimSpace(sql[i+2 : end])
			if value, ok := constFromExpr(expr, consts); ok {
				b.WriteString(formatConstForDiscovery(value))
			} else {
				b.WriteString(sql[i : end+1])
			}
			i = end + 1
			continue
		}
		end := i + 1
		for end < len(sql) && (isIdentPart(sql[end]) || sql[end] == '.') {
			end++
		}
		expr := sql[i+1 : end]
		if value, ok := constFromExpr(expr, consts); ok {
			b.WriteString(formatConstForDiscovery(value))
		} else {
			b.WriteString(sql[i:end])
		}
		i = end
	}
	return b.String()
}

func constFromExpr(expr string, consts map[string]string) (string, bool) {
	if expr == "" {
		return "", false
	}
	if strings.HasPrefix(expr, "Unsafe.") {
		expr = strings.TrimPrefix(expr, "Unsafe.")
	}
	if value, ok := consts[expr]; ok {
		return value, true
	}
	for name, value := range consts {
		if strings.EqualFold(name, expr) {
			return value, true
		}
	}
	return "", false
}

func formatConstForDiscovery(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "''"
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if !(isIdentPart(ch) || ch == '.' || ch == '`') {
			escaped := strings.ReplaceAll(value, "'", "''")
			return "'" + escaped + "'"
		}
	}
	return value
}

func normalizeDiscoveryTable(table string) string {
	trimmed := strings.TrimSpace(strings.Trim(table, "`\""))
	if strings.HasPrefix(trimmed, "${Unsafe.") && strings.HasSuffix(trimmed, "}") {
		trimmed = strings.TrimSuffix(strings.TrimPrefix(trimmed, "${Unsafe."), "}")
	}
	if strings.HasPrefix(trimmed, "$Unsafe.") {
		trimmed = strings.TrimPrefix(trimmed, "$Unsafe.")
	}
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" {
		return table
	}
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		if !(isIdentPart(ch) || ch == '.' || ch == '`' || ch == '"') {
			return table
		}
	}
	return strings.Trim(trimmed, "`\"")
}

func inferDiscoveryTable(sql string) string {
	lower := strings.ToLower(sql)
	idx := strings.Index(lower, " from ")
	if idx == -1 {
		if token := findUnsafeTableToken(sql); token != "" {
			return token
		}
		return ""
	}
	pos := idx + len(" from ")
	for pos < len(sql) && (sql[pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\n' || sql[pos] == '\r') {
		pos++
	}
	if pos >= len(sql) {
		return ""
	}
	if strings.HasPrefix(sql[pos:], "${Unsafe.") {
		end := strings.Index(sql[pos:], "}")
		if end == -1 {
			return ""
		}
		token := strings.TrimSpace(sql[pos+len("${Unsafe.") : pos+end])
		if token == "" {
			return ""
		}
		return token
	}
	if strings.HasPrefix(sql[pos:], "$Unsafe.") {
		start := pos + len("$Unsafe.")
		end := start
		for end < len(sql) && (isIdentPart(sql[end]) || sql[end] == '.') {
			end++
		}
		return strings.TrimSpace(sql[start:end])
	}
	if sql[pos] == '(' {
		depth := 1
		end := pos + 1
		for end < len(sql) && depth > 0 {
			switch sql[end] {
			case '(':
				depth++
			case ')':
				depth--
			}
			end++
		}
		if end > pos+1 {
			if nested := inferDiscoveryTable(sql[pos+1 : end-1]); nested != "" {
				return nested
			}
		}
		if token := findUnsafeTableToken(sql[pos:]); token != "" {
			return token
		}
		return ""
	}
	end := pos
	for end < len(sql) && (isIdentPart(sql[end]) || sql[end] == '.' || sql[end] == '`' || sql[end] == '"') {
		end++
	}
	return strings.TrimSpace(strings.Trim(sql[pos:end], "`\""))
}

func findUnsafeTableToken(sql string) string {
	if idx := strings.Index(sql, "${Unsafe."); idx != -1 {
		start := idx + len("${Unsafe.")
		end := strings.Index(sql[start:], "}")
		if end != -1 {
			return strings.TrimSpace(sql[start : start+end])
		}
	}
	if idx := strings.Index(sql, "$Unsafe."); idx != -1 {
		start := idx + len("$Unsafe.")
		end := start
		for end < len(sql) && (isIdentPart(sql[end]) || sql[end] == '.') {
			end++
		}
		return strings.TrimSpace(sql[start:end])
	}
	return ""
}

func removeExceptClauses(sql string) string {
	// Remove "EXCEPT col1, col2" patterns — these are datly-specific
	// Simple approach: remove " EXCEPT <identifier>(, <identifier>)*"
	result := sql
	for {
		lower := strings.ToLower(result)
		idx := strings.Index(lower, " except ")
		if idx == -1 {
			break
		}
		// Find end of EXCEPT clause (next keyword or end of identifier list)
		end := idx + len(" except ")
		for end < len(result) && (isIdentPart(result[end]) || result[end] == ',' || result[end] == ' ') {
			end++
		}
		result = result[:idx] + result[end:]
	}
	return result
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

func hasExceptClause(sql string) bool {
	lower := strings.ToLower(sql)
	return strings.Contains(lower, " except ")
}

// needsDiscovery returns true if the view SQL uses wildcards or has no explicit columns.
func needsDiscovery(aView *view.View) bool {
	if aView == nil {
		return false
	}
	if len(aView.Columns) == 0 {
		return true
	}
	if allPlaceholderColumns(aView.Columns) {
		return true
	}
	return usesWildcard(aView)
}

// stripTemplateVariables removes velocity/velty template constructs from SQL
// so it can be parsed and executed for column discovery.
// Handles: $variable, ${expression}, #if...#end, #foreach...#end, #set(...)
func stripTemplateVariables(sql string) string {
	var b strings.Builder
	b.Grow(len(sql))
	i := 0
	for i < len(sql) {
		// Handle # directives: #if, #foreach, #set, #end, #else, #elseif
		if sql[i] == '#' && i+1 < len(sql) {
			directive := matchDirective(sql, i)
			if directive != "" {
				// Skip entire directive line/block
				end := skipDirective(sql, i, directive)
				// Replace with space to preserve SQL structure
				b.WriteByte(' ')
				i = end
				continue
			}
		}
		// Handle $ variables: $name, $name.method(...), ${expression}
		if sql[i] == '$' && i+1 < len(sql) {
			next := sql[i+1]
			if next == '{' {
				// ${...} expression — find matching }
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
				// Replace with empty string or placeholder
				b.WriteString("''")
				i = j
				continue
			}
			if isIdentStart(next) {
				// $varName or $varName.method(...)
				j := i + 1
				for j < len(sql) && isIdentPart(sql[j]) {
					j++
				}
				hasMethodCall := false
				methodExpr := ""
				// Skip .method() chains
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
					} else {
						b.WriteString("")
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
		// Skip to end of line or matching paren
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
		// Skip to end of line
		for j < len(sql) && sql[j] != '\n' {
			j++
		}
		if j < len(sql) {
			j++
		}
		return j
	case directive == "#foreach" || directive == "#if":
		// Skip to matching #end
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
		// #else, #elseif, #end, #package, #import — skip to end of line
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

func allPlaceholderColumns(columns view.Columns) bool {
	if len(columns) == 0 {
		return false
	}
	for _, col := range columns {
		if col == nil {
			continue
		}
		name := strings.ToLower(col.Name)
		if !strings.HasPrefix(name, "col_") {
			return false
		}
	}
	return true
}

func lookupConnector(ctx context.Context, resource *view.Resource, aView *view.View) (*view.Connector, error) {
	if resource == nil {
		return nil, fmt.Errorf("shape column detector: missing resource for view %s", aView.Name)
	}
	if aView.Connector == nil {
		return nil, fmt.Errorf("shape column detector: missing connector for wildcard view %s", aView.Name)
	}
	connectors := view.ConnectorSlice(resource.Connectors).Index()
	connector := aView.Connector
	if connector.Ref != "" {
		lookup, err := connectors.Lookup(connector.Ref)
		if err != nil {
			return nil, fmt.Errorf("shape column detector: connector ref %s for view %s: %w", connector.Ref, aView.Name, err)
		}
		connector = lookup
	}
	if err := connector.Init(ctx, connectors); err != nil {
		return nil, fmt.Errorf("shape column detector: connector init for view %s: %w", aView.Name, err)
	}
	return connector, nil
}

func sourceSQL(aView *view.View) string {
	if aView.Template != nil && strings.TrimSpace(aView.Template.Source) != "" {
		return aView.Template.Source
	}
	return aView.Source()
}

func usesWildcard(aView *view.View) bool {
	if aView != nil && aView.Template == nil && strings.TrimSpace(aView.Table) != "" {
		return true
	}
	query := sourceSQL(aView)
	trimmed := strings.TrimSpace(strings.ToLower(query))
	if trimmed == "" {
		return false
	}
	if !strings.Contains(trimmed, "*") {
		return false
	}
	if !strings.HasPrefix(trimmed, "select") && !strings.HasPrefix(trimmed, "with") {
		return true
	}
	parsed, err := sqlparser.ParseQuery(query)
	if err != nil {
		return true
	}
	return sqlparser.NewColumns(parsed.List).IsStarExpr()
}

func columnsFromSchema(aView *view.View) view.Columns {
	if aView == nil || aView.Schema == nil {
		return nil
	}
	rType := aView.Schema.Type()
	if rType == nil {
		return nil
	}
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		return nil
	}
	result := make(view.Columns, 0, rType.NumField())
	appendSchemaColumns(rType, "", &result)
	if allPlaceholderColumns(result) {
		return nil
	}
	return result
}

func appendSchemaColumns(rType reflect.Type, ns string, columns *view.Columns) {
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		if field.PkgPath != "" {
			continue
		}
		if field.Anonymous {
			inner := field.Type
			for inner.Kind() == reflect.Ptr {
				inner = inner.Elem()
			}
			if inner.Kind() == reflect.Struct {
				appendSchemaColumns(inner, ns, columns)
			}
			continue
		}
		if shouldSkipSchemaField(field) {
			continue
		}
		tag := io.ParseTag(field.Tag)
		if tag != nil && tag.Transient {
			continue
		}
		name := field.Name
		if tag != nil && tag.Column != "" {
			name = tag.Column
		}
		if tag != nil && tag.Ns != "" {
			name = tag.Ns + name
		} else if ns != "" {
			name = ns + name
		}
		columnType := field.Type
		nullable := false
		if columnType.Kind() == reflect.Ptr {
			nullable = true
			columnType = columnType.Elem()
		}
		*columns = append(*columns, view.NewColumn(name, columnType.String(), columnType, nullable, view.WithColumnTag(string(field.Tag))))
	}
}

func shouldSkipSchemaField(field reflect.StructField) bool {
	if field.Name == "-" {
		return true
	}
	rawTag := string(field.Tag)
	if strings.Contains(rawTag, `view:"`) || strings.Contains(rawTag, `on:"`) {
		return true
	}
	if strings.Contains(rawTag, `sqlx:"-"`) {
		return true
	}
	return false
}

func mergePreservingOrder(base, discovered view.Columns) view.Columns {
	if len(base) == 0 {
		return discovered
	}
	if len(discovered) == 0 {
		return base
	}
	seen := map[string]*view.Column{}
	for _, item := range discovered {
		if item == nil {
			continue
		}
		seen[strings.ToLower(item.Name)] = item
	}
	result := make(view.Columns, 0, len(base)+len(discovered))
	for _, item := range base {
		if item == nil {
			continue
		}
		if fresh, ok := seen[strings.ToLower(item.Name)]; ok {
			delete(seen, strings.ToLower(item.Name))
			item.DataType = firstNonEmpty(fresh.DataType, item.DataType)
			item.SetColumnType(firstType(fresh.ColumnType(), item.ColumnType()))
			item.Nullable = fresh.Nullable
			if item.DatabaseColumn == "" {
				item.DatabaseColumn = fresh.DatabaseColumn
			}
		}
		result = append(result, item)
	}
	for _, item := range discovered {
		if item == nil {
			continue
		}
		if _, ok := seen[strings.ToLower(item.Name)]; !ok {
			continue
		}
		result = append(result, item)
		delete(seen, strings.ToLower(item.Name))
	}
	return result
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentPart(ch byte) bool {
	return isIdentStart(ch) || (ch >= '0' && ch <= '9')
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func firstType(values ...reflect.Type) reflect.Type {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}
