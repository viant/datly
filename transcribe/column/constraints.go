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
	session, err := config.Session(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("load SQLX metadata session: %w", err)
	}
	columns, err := config.Columns(ctx, session, db, table)
	if err != nil {
		return nil, fmt.Errorf("load SQLX table metadata for %q: %w", table, err)
	}
	result := constraintsFromColumns(columns)
	keys := make([]sink.Key, 0)
	err = metadata.New().Info(ctx, db, info.KindForeignKeys, &keys,
		option.NewArgs(session.Catalog, session.Schema, table))
	if err != nil {
		// Some SQLX products expose columns but not foreign-key metadata. PK,
		// nullability, defaults and uniqueness remain authoritative in that case.
		if strings.Contains(strings.ToLower(err.Error()), "unsupported") {
			return result, nil
		}
		return nil, fmt.Errorf("load SQLX foreign keys for %q: %w", table, err)
	}
	for index := range keys {
		key := &keys[index]
		name := normalizedName(key.Column)
		if name == "" || strings.TrimSpace(key.ReferenceTable) == "" || strings.TrimSpace(key.ReferenceColumn) == "" {
			continue
		}
		constraint := result[name]
		constraint.reference = &tableReference{
			schema: strings.TrimSpace(key.ReferenceSchema),
			table:  strings.TrimSpace(key.ReferenceTable),
			column: strings.TrimSpace(key.ReferenceColumn),
		}
		result[name] = constraint
	}
	return result, nil
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
	parsed, err := sqlparser.ParseQuery(text)
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
			parsed, err := sqlparser.ParseQuery(trimParentheses(with.Raw))
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
	parsed, err := sqlparser.ParseQuery(trimParentheses(raw))
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

func applyTableConstraints(columns []*spec.Column, constraints map[string]tableConstraint, lineage *projectionLineage) {
	if len(columns) == 0 || len(constraints) == 0 || lineage == nil {
		return
	}
	for _, column := range columns {
		if column == nil {
			continue
		}
		output := normalizedName(column.Name)
		sourceName, ok := lineage.direct[output]
		if !ok && lineage.wildcard && !lineage.blocked[output] {
			sourceName = output
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
		applyReferenceConstraint(column, constraint.reference)
	}
}

func applyReferenceConstraint(column *spec.Column, reference *tableReference) {
	if column == nil || reference == nil {
		return
	}
	parsed := tags.NewTags(strings.TrimSpace(column.Tag))
	sqlxTag := parsed.Lookup(sqlio.TagSqlx)
	if sqlxTag == nil {
		mapping := firstValue(column.Source, column.Name)
		parsed.Set(sqlio.TagSqlx, mapping)
		sqlxTag = parsed.Lookup(sqlio.TagSqlx)
	}
	if sqlxTag == nil {
		return
	}
	authored := sqlio.ParseTag(reflect.StructTag(parsed.Stringify()))
	if authored.RefDb == "" && reference.schema != "" {
		sqlxTag.Append("refDb=" + reference.schema)
	}
	if authored.RefTable == "" {
		sqlxTag.Append("refTable=" + reference.table)
	}
	if authored.RefColumn == "" {
		sqlxTag.Append("refColumn=" + reference.column)
	}
	column.Tag = parsed.Stringify()
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
