package column

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	sqlio "github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"github.com/viant/tagly/tags"
)

type tableConstraint struct {
	name          string
	primaryKey    bool
	autoIncrement bool
	unique        bool
	notNull       bool
	defaultValue  *string
	reference     *tableReference
}

type tableReference struct {
	schema string
	table  string
	column string
}

type projectionLineage struct {
	names    map[string]string
	direct   map[string]string
	blocked  map[string]bool
	wildcard bool
}

func loadTableConstraints(ctx context.Context, db *sql.DB, table string) (map[string]tableConstraint, error) {
	return (&discoveryMetadata{}).loadTableConstraints(ctx, db, table)
}

func (m *discoveryMetadata) loadTableConstraints(ctx context.Context, db *sql.DB, table string) (map[string]tableConstraint, error) {
	product, err := m.product(ctx, db)
	if err != nil {
		return nil, err
	}
	sourceTable := table
	session, table, err := m.tableSession(ctx, db, product, table)
	if err != nil {
		return nil, fmt.Errorf("load SQLX metadata session: %w", err)
	}
	txOptions, err := discoveryTransactionOptions(ctx, db)
	if err != nil {
		return nil, err
	}
	columns, err := config.Columns(ctx, session, db, table, append(txOptions, product)...)
	if err != nil {
		return nil, fmt.Errorf("load SQLX table metadata for %q: %w", table, err)
	}
	result := constraintsFromColumns(columns)
	keys := make([]sink.Key, 0)
	err = metadata.New().Info(ctx, db, info.KindForeignKeys, &keys, append(append([]option.Option(nil), txOptions...), product,
		option.NewArgs(session.Catalog, session.Schema, table))...)
	if err != nil {
		// Some SQLX products expose columns but not foreign-key metadata. PK,
		// nullability, defaults and uniqueness remain authoritative in that case.
		if strings.Contains(strings.ToLower(err.Error()), "unsupported") {
			return result, nil
		}
		return nil, fmt.Errorf("load SQLX foreign keys for %q: %w", table, err)
	}
	applyForeignKeyConstraints(result, keys, session.Schema, sourceTable)
	return result, nil
}

// A complete BigQuery path already supplies the metadata namespace. Keep it
// local to this table read; it is not the connection's default session.
func (m *discoveryMetadata) tableSession(ctx context.Context, db *sql.DB, product *database.Product, table string) (*sink.Session, string, error) {
	var parts []string
	if strings.EqualFold(product.Name, "BigQuery") {
		var err error
		parts, err = (sqlparser.TableIdentifierParser{Product: product.Name}).Parts(table)
		if err != nil {
			return nil, "", fmt.Errorf("parse metadata table: %w", err)
		}
		if len(parts) == 3 {
			return &sink.Session{Catalog: parts[0], Schema: parts[1]}, parts[2], nil
		}
	}
	session, err := m.session(ctx, db, product)
	if err != nil {
		return nil, "", err
	}
	if len(parts) != 0 {
		copy := *session
		if len(parts) == 2 {
			copy.Schema = parts[0]
		}
		return &copy, parts[len(parts)-1], nil
	}
	return session, table, nil
}

func applyForeignKeyConstraints(result map[string]tableConstraint, keys []sink.Key, discoverySchema, sourceTable string) {
	for index := range keys {
		key := &keys[index]
		name := normalizedName(key.Column)
		if name == "" || strings.TrimSpace(key.ReferenceTable) == "" || strings.TrimSpace(key.ReferenceColumn) == "" {
			continue
		}
		constraint := result[name]
		constraint.reference = &tableReference{
			schema: inferredReferenceSchema(key, discoverySchema, sourceTable),
			table:  strings.TrimSpace(key.ReferenceTable),
			column: strings.TrimSpace(key.ReferenceColumn),
		}
		result[name] = constraint
	}
}

func inferredReferenceSchema(key *sink.Key, discoverySchema, sourceTable string) string {
	// Only authoritative same-schema metadata for an unqualified source may
	// become runtime-local. Schema names are case-sensitive provenance; unknown
	// or cross-schema references and explicit source qualification stay intact.
	if strings.TrimSpace(discoverySchema) != "" && key.Schema == discoverySchema && key.ReferenceSchema == discoverySchema &&
		strings.TrimSpace(sourceTable) != "" && !strings.Contains(sourceTable, ".") {
		return ""
	}
	return strings.TrimSpace(key.ReferenceSchema)
}

func constraintsFromColumns(columns []sink.Column) map[string]tableConstraint {
	result := make(map[string]tableConstraint, len(columns))
	for index := range columns {
		column := &columns[index]
		name := normalizedName(column.Name)
		if name == "" {
			continue
		}
		primaryKey := strings.EqualFold(strings.TrimSpace(column.Key), "PRI")
		constraint := tableConstraint{
			name:          column.Name,
			primaryKey:    primaryKey,
			autoIncrement: column.Autoincrement(),
			unique:        !primaryKey && column.IsUnique(),
			notNull:       column.IsNotNull(),
		}
		if column.Default != nil && strings.TrimSpace(*column.Default) != "" {
			value := *column.Default
			constraint.defaultValue = &value
		}
		result[name] = constraint
	}
	return result
}

func directProjectionLineage(source *spec.ViewSource) (*projectionLineage, error) {
	result := &projectionLineage{names: map[string]string{}, direct: map[string]string{}, blocked: map[string]bool{}}
	if source == nil || strings.TrimSpace(source.Table) == "" {
		return result, nil
	}
	text := strings.TrimSpace(source.SQL)
	if text == "" {
		result.wildcard = true
		return result, nil
	}
	parsed, err := parseProjectionAnalysis(text)
	if err != nil {
		return nil, fmt.Errorf("parse table projection lineage: %w", err)
	}
	return (projectionLineageResolver{table: source.Table}).query(parsed)
}

type projectionLineageResolver struct {
	table string
	withs query.WithSelects
	depth int
}

func (r projectionLineageResolver) query(parsed *query.Select) (*projectionLineage, error) {
	result := &projectionLineage{names: map[string]string{}, direct: map[string]string{}, blocked: map[string]bool{}}
	if parsed == nil || parsed.Union != nil {
		return result, nil
	}
	if r.depth > 32 {
		return nil, fmt.Errorf("projection lineage is recursive")
	}
	r.depth++
	r.withs = append(append(query.WithSelects(nil), parsed.WithSelects...), r.withs...)
	source, err := r.source(parsed.From.X)
	if err != nil {
		return nil, err
	}
	allowedNamespaces := map[string]bool{}
	rootNamespaces := []string{parsed.From.Alias}
	if strings.TrimSpace(parsed.From.Alias) == "" {
		table, _, err := sqlparser.SourceTable(parsed.From.X)
		if err != nil {
			return nil, err
		}
		rootNamespaces = []string{table, tableBase(table)}
	}
	for _, namespace := range rootNamespaces {
		if namespace = normalizedName(namespace); namespace != "" {
			allowedNamespaces[namespace] = true
		}
	}
	requireQualifier := len(parsed.Joins) > 0
	for _, item := range parsed.List {
		if item == nil {
			continue
		}
		namespace, wildcard := projectionWildcard(item.Expr)
		if wildcard && lineageNamespaceAllowed(namespace, allowedNamespaces, requireQualifier) {
			result.wildcard = source.wildcard
			for output, name := range source.direct {
				result.direct[output] = name
				result.names[output] = source.names[output]
			}
			for output := range source.blocked {
				result.blocked[output] = true
			}
		}
	}
	for _, item := range parsed.List {
		if item == nil {
			continue
		}
		if _, wildcard := projectionWildcard(item.Expr); wildcard {
			continue
		}
		column := sqlparser.NewColumn(item)
		output := normalizedName(column.Identity())
		if output == "" {
			continue
		}
		if !lineageNamespaceAllowed(column.Namespace, allowedNamespaces, requireQualifier) || strings.TrimSpace(column.Expression) != "" {
			result.blocked[output] = true
			delete(result.direct, output)
			continue
		}
		name := normalizedName(column.Name)
		physical, found := source.direct[name]
		if !found && source.wildcard && !source.blocked[name] {
			physical, found = name, name != ""
		}
		if !found || source.blocked[name] {
			result.blocked[output] = true
			delete(result.direct, output)
			continue
		}
		result.direct[output] = physical
		result.names[output] = column.Identity()
	}
	return result, nil
}

func (r projectionLineageResolver) source(value node.Node) (*projectionLineage, error) {
	empty := &projectionLineage{names: map[string]string{}, direct: map[string]string{}, blocked: map[string]bool{}}
	if identifier, ok := value.(*expr.Ident); ok {
		for _, with := range r.withs {
			if with == nil || !strings.EqualFold(with.Alias, identifier.Name) {
				continue
			}
			if with.X != nil {
				return r.query(with.X)
			}
			parsed, err := parseProjectionAnalysis(trimParentheses(with.Raw))
			if err != nil {
				return nil, err
			}
			return r.query(parsed)
		}
	}
	if table, _, err := sqlparser.SourceTable(value); err != nil {
		return nil, err
	} else if table != "" {
		empty.wildcard = sameTable(table, r.table)
		return empty, nil
	}
	var raw string
	switch actual := value.(type) {
	case *query.Select:
		return r.query(actual)
	case *expr.Raw:
		raw = actual.Raw
	case *expr.Parenthesis:
		raw = actual.Raw
	default:
		return empty, nil
	}
	if strings.TrimSpace(raw) == "" {
		return empty, nil
	}
	parsed, err := parseProjectionAnalysis(trimParentheses(raw))
	if err != nil {
		return nil, err
	}
	return r.query(parsed)
}

func projectionWildcard(projection node.Node) (string, bool) {
	switch actual := projection.(type) {
	case *expr.Star:
		switch qualifier := actual.X.(type) {
		case *expr.Selector:
			if qualifier.Name == "*" {
				return "", true
			}
			return qualifier.Name, true
		case *expr.Ident:
			if qualifier.Name == "*" {
				return "", true
			}
			return qualifier.Name, true
		}
		return "", true
	case *expr.Selector:
		_, wildcard := actual.X.(*expr.Star)
		return actual.Name, wildcard
	default:
		return "", false
	}
}

func applyTableConstraints(columns []*spec.Column, constraints map[string]tableConstraint, lineage *projectionLineage, sourceTables ...string) {
	if len(columns) == 0 || len(constraints) == 0 || lineage == nil {
		return
	}
	for _, column := range columns {
		if column == nil {
			continue
		}
		output := normalizedName(column.Name)
		sourceName, ok := lineage.direct[output]
		resultName := lineage.names[output]
		if !ok && column.Source != "" {
			// A configuration-only outer rename keeps the original SQL output in
			// Source; prove its lineage before applying physical table constraints.
			sourceOutput := normalizedName(column.Source)
			sourceName, ok = lineage.direct[sourceOutput]
			resultName = lineage.names[sourceOutput]
		}
		// Outer Go-field aliases do not rename the SQL wildcard's result.
		// Resolve physical facts using that retained result identity, just as
		// the explicit projection lookup above does.
		wildcardOutput := normalizedName(firstValue(column.Source, column.Name))
		if !ok && lineage.wildcard && !lineage.blocked[wildcardOutput] {
			sourceName = wildcardOutput
			ok = sourceName != ""
		}
		if !ok {
			continue
		}
		constraint, ok := constraints[sourceName]
		if !ok {
			continue
		}
		// SQL output aliases remain canonical names; DML mapping follows the
		// already-proven physical lineage, unless the author supplied a source.
		if column.Source == column.Name && constraint.name != "" {
			column.Source = constraint.name
		}
		column.PrimaryKey = column.PrimaryKey || constraint.primaryKey
		column.AutoIncrement = column.AutoIncrement || constraint.autoIncrement
		column.Unique = column.Unique || constraint.unique
		column.NotNull = column.NotNull || constraint.notNull
		if column.Default == nil && constraint.defaultValue != nil {
			value := *constraint.defaultValue
			column.Default = &value
		}
		// Carry SQL-proven result identity across Go emission. An inferred Go
		// field name alone cannot supply that provenance at runtime.
		if normalizedName(column.Source) == sourceName && resultName != "" && !strings.EqualFold(column.Source, resultName) {
			parsed := tags.NewTags(strings.TrimSpace(column.Tag))
			if parsed.Lookup(sqlio.TagSqlx) == nil {
				parsed.Set(sqlio.TagSqlx, physicalResultMapping(column, resultName))
				column.Tag = parsed.Literal()
			}
		}
		applyReferenceConstraint(column, constraint.reference, resultName)
		if constraint.unique && len(sourceTables) != 0 {
			applyUniqueTableConstraint(column, sourceTables[0], resultName)
		}
	}
}

func physicalResultMapping(column *spec.Column, resultName string) string {
	mapping := firstValue(column.Source, column.Name)
	if resultName = strings.TrimSpace(resultName); resultName != "" && !strings.EqualFold(mapping, resultName) {
		mapping += "|" + resultName
	}
	return mapping
}

// SQLX compiles uniqueness from scalar tags, independently of view metadata.
// The caller supplies only the table whose physical projection was proven.
func applyUniqueTableConstraint(column *spec.Column, table, resultName string) {
	if table = strings.TrimSpace(table); table == "" {
		return
	}
	parsed := tags.NewTags(strings.TrimSpace(column.Tag))
	sqlxTag := parsed.Lookup(sqlio.TagSqlx)
	metadata := sqlio.ParseTag(reflect.StructTag(parsed.Stringify()))
	if metadata.Transient || metadata.Table != "" {
		return
	}
	if sqlxTag != nil {
		blocked := false
		_ = sqlxTag.Values.Match(func(value string) error {
			key := strings.TrimSpace(value)
			if i := strings.IndexByte(key, '='); i >= 0 {
				key = strings.TrimSpace(key[:i])
			}
			if strings.EqualFold(key, "table") || strings.EqualFold(key, "uniqueDep") || strings.EqualFold(key, "unique") && !metadata.IsUnique {
				blocked = true
			}
			return nil
		})
		if blocked {
			return
		}
	} else {
		parsed.Set(sqlio.TagSqlx, physicalResultMapping(column, resultName))
		sqlxTag = parsed.Lookup(sqlio.TagSqlx)
	}
	sqlxTag.Append("table=" + table)
	column.Tag = parsed.Literal()
}

func applyReferenceConstraint(column *spec.Column, reference *tableReference, resultName string) {
	if column == nil || reference == nil {
		return
	}
	parsed := tags.NewTags(strings.TrimSpace(column.Tag))
	sqlxTag := parsed.Lookup(sqlio.TagSqlx)
	if sqlxTag == nil {
		parsed.Set(sqlio.TagSqlx, physicalResultMapping(column, resultName))
		sqlxTag = parsed.Lookup(sqlio.TagSqlx)
	}
	if sqlxTag == nil {
		return
	}
	authored := sqlio.ParseTag(reflect.StructTag(parsed.Stringify()))
	if authored.Transient {
		// Ignored SQLX fields have no executable FK mapping. Appending options
		// would turn the exact exclusion tag into a mapped column named "-".
		return
	}
	if authored.RefDb == "" && reference.schema != "" {
		sqlxTag.Append("refDb=" + reference.schema)
	}
	if authored.RefTable == "" {
		sqlxTag.Append("refTable=" + reference.table)
	}
	if authored.RefColumn == "" {
		sqlxTag.Append("refColumn=" + reference.column)
	}
	// Preserve authored empty values such as json:"": they suppress inferred
	// omission without fixing a public name. Stringify would discard them.
	column.Tag = parsed.Literal()
}

func lineageNamespaceAllowed(namespace string, allowed map[string]bool, requireQualifier bool) bool {
	namespace = normalizedName(namespace)
	if namespace == "" {
		return !requireQualifier
	}
	return allowed[namespace]
}

func sameTable(actual, expected string) bool {
	actual = normalizedTable(actual)
	expected = normalizedTable(expected)
	return actual != "" && actual == expected
}

func normalizedTable(value string) string {
	value = strings.TrimSpace(value)
	parts := strings.Split(value, ".")
	for index, part := range parts {
		parts[index] = strings.Trim(strings.TrimSpace(part), "`\"[]")
	}
	return strings.ToLower(strings.Join(parts, "."))
}

func tableBase(value string) string {
	parts := strings.Split(normalizedTable(value), ".")
	return parts[len(parts)-1]
}

func normalizedName(value string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(value), "`\"[]"))
}
