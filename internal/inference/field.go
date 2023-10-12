package inference

import (
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/structology/format/text"
	"reflect"
	"strings"
)

type Field struct {
	view.Field
	Skipped    bool
	Column     *sqlparser.Column
	Pk         *sink.Key
	Tags       Tags
	Ptr        bool
	ColumnCase text.CaseFormat
	Relation   string
}

func NewField(rField *reflect.StructField) *Field {
	field := &Field{}
	field.Name = rField.Name
	rType := rField.Type
	cardinality := state.One
	if rType.Kind() == reflect.Slice {
		cardinality = state.Many
		rType = rType.Elem()
	}
	if field.Ptr = rType.Kind() == reflect.Ptr; field.Ptr {
		rType = rType.Elem()
	}
	field.Schema = state.NewSchema(rType)
	field.Schema.Cardinality = cardinality
	field.Schema.DataType = rType.Name()
	if typeName, _ := rField.Tag.Lookup("typeName"); typeName != "" {
		field.Schema.DataType = typeName
	}

	sqlxTag := SqlxTag(rField.Tag)
	if sqlxTag != nil && sqlxTag.Column != "" {
		column := sqlparser.Column{Name: sqlxTag.Column}
		if sqlxTag.Autoincrement {
			column.IsAutoincrement = sqlxTag.Autoincrement
		}
		column.IsNullable = !sqlxTag.Required
		column.Type = field.Schema.DataType
		column.Tag = strings.Trim(string(rField.Tag), "`")
		field.Column = &column
	}
	return field
}

func (f *Field) EnsureSchema() *state.Schema {
	if f.Schema != nil {
		return f.Schema
	}
	f.Schema = &state.Schema{}
	return f.Schema
}
