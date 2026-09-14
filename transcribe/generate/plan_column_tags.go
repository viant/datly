package generate

import (
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	sqlio "github.com/viant/sqlx/io"
	"github.com/viant/tagly/tags"
)

func scalarColumnFieldTag(column *spec.Column, source string, includeVelty bool) string {
	parsed := tags.NewTags(strings.TrimSpace(column.Tag))
	sqlxTag := parsed.Lookup(sqlio.TagSqlx)
	if sqlxTag == nil {
		parsed.Set(sqlio.TagSqlx, source)
		sqlxTag = parsed.Lookup(sqlio.TagSqlx)
	}
	if sqlxTag == nil {
		return withVeltyNames(parsed.Stringify(), source, typecatalog.FieldName(column.Name), includeVelty)
	}
	metadata := sqlio.ParseTag(reflect.StructTag(parsed.Stringify()))
	if metadata.Transient {
		return parsed.Stringify()
	}
	if column.NotNull && !metadata.Required && !hasSQLXOption(sqlxTag.Values, "required") {
		sqlxTag.Append("required=true")
	}
	if column.PrimaryKey && !metadata.PrimaryKey && !hasSQLXOption(sqlxTag.Values, "primaryKey") {
		sqlxTag.Append("primaryKey=true")
	}
	if column.AutoIncrement && !metadata.Autoincrement && !hasSQLXOption(sqlxTag.Values, "autoincrement", "generator") {
		sqlxTag.Append("autoincrement=true")
	}
	if column.Unique && !metadata.IsUnique && !hasSQLXOption(sqlxTag.Values, "unique", "uniqueDep") {
		sqlxTag.Append("unique=true")
	}
	return withVeltyNames(parsed.Stringify(), source, typecatalog.FieldName(column.Name), includeVelty)
}

func hasSQLXOption(values tags.Values, names ...string) bool {
	wanted := make(map[string]bool, len(names))
	for _, name := range names {
		wanted[strings.ToLower(strings.TrimSpace(name))] = true
	}
	found := false
	_ = values.Match(func(value string) error {
		key := strings.TrimSpace(value)
		if index := strings.IndexByte(key, '='); index >= 0 {
			key = strings.TrimSpace(key[:index])
		}
		if wanted[strings.ToLower(key)] {
			found = true
		}
		return nil
	})
	return found
}

func hasStructTag(raw, name string) bool {
	return tags.NewTags(strings.TrimSpace(raw)).Lookup(name) != nil
}
