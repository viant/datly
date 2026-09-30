package typecatalog

import (
	sqlxio "github.com/viant/sqlx/io"
	tagvalues "github.com/viant/tagly/tags"
	"reflect"
	"strings"
)

// SQLXPrimaryKey resolves the generated contract's identity using the native
// SQLX parser. An explicit mapping overrides discovery in both planning and
// runtime; discovery is retained when the option is absent.
func SQLXPrimaryKey(tag reflect.StructTag, inferred bool) bool {
	value := tag.Get("sqlx")
	if value == "-" {
		return false
	}
	_, options := tagvalues.Values(value).Name()
	declared := false
	_ = options.MatchRawPairs(func(key, value string) error {
		if strings.EqualFold(strings.TrimSpace(key), "primaryKey") {
			declared = true
		}
		return nil
	})
	if declared {
		return sqlxio.ParseTag(tag).PrimaryKey
	}
	return inferred
}
