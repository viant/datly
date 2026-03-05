package column

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/view"
	viewcolumn "github.com/viant/datly/view/column"
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
	query := discoverySQL(aView)
	sqlColumns, err := viewcolumn.Discover(ctx, db, aView.Table, query)
	if err != nil {
		return nil, fmt.Errorf("shape column detector: discover failed for view %s: %w", aView.Name, err)
	}
	return view.NewColumns(sqlColumns, aView.ColumnsConfig), nil
}

// discoverySQL returns SQL suitable for column discovery.
// Strategy:
//  1. Strip template variables ($var, #if...#end, ${expr})
//  2. Inject 1=0 into every SELECT in the query (CTEs, UNIONs, subqueries)
//     This ensures zero rows scanned — safe for BigQuery (no full scan cost)
//  3. Fall back to table name if parsing/falsification fails
func discoverySQL(aView *view.View) string {
	raw := sourceSQL(aView)
	table := strings.TrimSpace(aView.Table)
	if raw == "" {
		return table
	}
	// If SQL has template variables, EXCEPT, or other datly extensions,
	// use table-based discovery which is always safe and accurate
	if table != "" && (hasTemplateVariables(raw) || hasExceptClause(raw)) {
		return table
	}
	// For clean SQL without templates, try to falsify for column type inference
	cleaned := strings.TrimSpace(raw)
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
				// Skip .method() chains
				for j < len(sql) && sql[j] == '.' {
					j++
					for j < len(sql) && isIdentPart(sql[j]) {
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
					}
				}
				b.WriteString("''")
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
