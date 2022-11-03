package cmd

import (
	"fmt"
	errUtils "github.com/viant/datly/err-utils"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata/sink"
	"reflect"
	"strings"
)

type (
	TableMetaRegistry struct {
		metas map[string]*TableMeta
	}

	TableMeta struct {
		index   map[string]int
		Columns []*ColumnMeta
	}

	ColumnIndex map[string]*ColumnMeta
	ColumnMeta  struct {
		Name string
		Type reflect.Type
	}
)

func NewTableMetaRegistry() *TableMetaRegistry {
	return &TableMetaRegistry{
		metas: map[string]*TableMeta{},
	}
}

func (t *TableMetaRegistry) Indexed(tableName string) bool {
	meta := t.TableMeta(tableName)
	return len(meta.Columns) > 0
}

func (t *TableMetaRegistry) TableMeta(tableName string) *TableMeta {
	if meta, ok := t.metas[tableName]; ok {
		return meta
	}

	meta := &TableMeta{
		index: map[string]int{},
	}
	t.metas[tableName] = meta
	return meta
}

func (m *TableMeta) AddIoColumns(columns []io.Column) {
	for _, column := range columns {
		m.addColumn(&ColumnMeta{
			Name: column.Name(),
			Type: column.ScanType(),
		})

		m.Columns = append(m.Columns)
	}
}

func (m *TableMeta) AddSinkColumns(columns []sink.Column) error {
	var errors []error

	for _, column := range columns {
		rType, err := view.GetOrParseType(map[string]reflect.Type{}, column.Type)
		if err != nil {
			errors = append(errors, fmt.Errorf("couldn't convert %v column type %v due to the %w", column.Name, column.Type, err))
			continue
		}

		m.addColumn(&ColumnMeta{
			Name: column.Name,
			Type: rType,
		})
	}

	return errUtils.CombineErrors("errors occured while detecting table column types: ", errors)
}

func (m *TableMeta) addColumn(column *ColumnMeta) {
	column.Type = normalizeType(column.Type)

	//	columnType := column.ScanType().String()
	//	if strings.HasPrefix(columnType, "*") {
	//		columnType = columnType[1:]
	//	}
	//	if columnType == "sql.RawBytes" {
	//		columnType = "string"
	//	}
	//	if strings.Contains(columnType, "int") {
	//		columnType = "int"
	//	}
	//	key := prefix
	//	if key != "" {
	//		key += "."
	//	}

	index, ok := m.index[column.Name]
	if !ok {
		m.index[column.Name] = len(m.Columns)
		m.Columns = append(m.Columns, column)
		return
	}

	if strings.Contains(strings.ToLower(m.Columns[index].Type.String()), "interface") {
		m.Columns[index] = column
	}
}

func normalizeType(rType reflect.Type) reflect.Type {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	columnType := rType.String()
	switch columnType {
	case "sql.RawBytes":
		rType = reflect.TypeOf("")
	default:
		if strings.Contains(columnType, "int") && !strings.Contains(columnType, "interface") {
			rType = reflect.TypeOf(0)
		}
	}

	return rType
}

func (m *TableMeta) IndexColumns(alias string) ColumnIndex {
	prefix := ""
	if alias != "" {
		prefix = alias + "."
	}

	result := ColumnIndex{}
	for i, column := range m.Columns {
		result[strings.ToLower(prefix+column.Name)] = m.Columns[i]
	}

	return result
}

func (c ColumnIndex) Merge(with ColumnIndex) ColumnIndex {
	for columnName := range with {
		c[columnName] = with[columnName]
	}

	return c
}
