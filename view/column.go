package view

import (
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"github.com/viant/sqlx/types"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
	"time"
)

type foo struct {
	Interface interface{}
}

//Column represents view View column
type Column struct {
	Name           string `json:",omitempty"`
	DataType       string `json:",omitempty"`
	Expression     string `json:",omitempty"`
	Filterable     bool   `json:",omitempty"`
	Nullable       bool   `json:",omitempty"`
	Default        string `json:",omitempty"`
	Format         string `json:",omitempty"`
	Codec          *Codec `json:",omitempty"`
	DatabaseColumn string `json:",omitempty"`
	IndexedBy      string `json:",omitempty"`

	rType         reflect.Type
	tag           *io.Tag
	sqlExpression string
	field         *reflect.StructField
	initialized   bool
	_fieldName    string
}

//SqlExpression builds column sql expression if any expression specified in format: Expression AS Name
func (c *Column) SqlExpression() string {
	return c.sqlExpression
}

func ParseType(dataType string, typeLookup xreflect.TypeLookupFn) (reflect.Type, error) {
	precisionIndex := strings.Index(dataType, "(")
	if precisionIndex != -1 {
		dataType = dataType[:precisionIndex]
	}

	switch strings.ToLower(dataType) {
	case "int", "integer", "bigint", "smallint", "unsiged tinyint", "tinyint", "int64", "int32", "int16", "int8", "uint", "uint8", "uint16", "uint32", "uint64", "binary":
		return reflect.TypeOf(0), nil
	case "float", "float64", "numeric", "decimal", "double":
		return reflect.TypeOf(0.0), nil
	case "bool", "boolean":
		return reflect.TypeOf(false), nil
	case "types.Bool", "Bool":
		return reflect.TypeOf(types.Bool(true)), nil
	case "bit":
		return reflect.TypeOf(""), nil
	case "string", "varchar", "char", "text", "longtext", "longblob", "mediumblob", "mediumtext", "blob", "tinytext":
		return reflect.TypeOf(""), nil
	case "date", "time", "timestamp", "datetime":
		return reflect.TypeOf(time.Time{}), nil
	case "sql.RawBytes":
		return reflect.TypeOf(""), nil
	case "interface":
		t := reflect.ValueOf(interface{}(foo{})).FieldByName("Interface").Type()
		return t, nil
	}

	return xreflect.ParseWithLookup(dataType, true, typeLookup)
}

//ColumnName returns Column Name
func (c *Column) ColumnName() string {
	return c.Name
}

//Init initializes Column
func (c *Column) Init(resource *Resource, caser format.Case, allowNulls bool, config *ColumnConfig) error {
	if c.initialized {
		return nil
	}

	c.initialized = true
	if config != nil {
		c.inherit(config)
	}

	if c.DatabaseColumn == "" {
		c.DatabaseColumn = c.Name
	}

	if c.Name == "" {
		return fmt.Errorf("column name was empty")
	}

	nonPtrType := c.rType
	for nonPtrType != nil && nonPtrType.Kind() == reflect.Ptr {
		nonPtrType = nonPtrType.Elem()
	}

	if nonPtrType == nil || nonPtrType.Kind() == reflect.Interface {
		rType, err := ParseType(c.DataType, resource._types.LookupType)
		if err != nil && c.rType == nil {
			return err
		}

		if rType != nil {
			c.rType = rType
		}
	}

	if err := c.buildSQLExpression(allowNulls); err != nil {
		return err
	}

	c._fieldName = caser.Format(c.Name, format.CaseUpperCamel)

	if c.Codec != nil {
		if err := c.Codec.Init(resource, nil, nil); err != nil {
			return err
		}
	}

	return nil
}

func (c *Column) buildSQLExpression(allowNulls bool) error {
	defaultValue := c.defaultValue(c.rType)
	c.sqlExpression = c.DatabaseColumn
	if c.Expression != "" {
		c.sqlExpression = c.Expression
	}

	if defaultValue != "" && !allowNulls {
		c.sqlExpression = "COALESCE(" + c.sqlExpression + "," + defaultValue + ") AS " + c.Name
	} else if c.Expression != "" {
		c.sqlExpression = c.sqlExpression + " AS " + c.Name
	}

	return nil
}

func (c *Column) setField(field reflect.StructField) {
	c.field = &field
	c.tag = io.ParseTag(field.Tag.Get(option.TagSqlx))
}

func (c *Column) defaultValue(rType reflect.Type) string {
	if !c.Nullable {
		return ""
	}

	switch rType.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8,
		reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return `0`
	case reflect.Float64, reflect.Float32:
		return `0`
	case reflect.String:
		return `""`
	case reflect.Bool:
		return `false`
	default:
		return ""
	}
}

func (c *Column) FieldName() string {
	return c._fieldName
}

func (c *Column) ColumnType() reflect.Type {
	return c.rType
}

func (c *Column) inherit(config *ColumnConfig) {
	if config.Codec != nil {
		c.Codec = config.Codec
	}

	if config.DataType != nil {
		c.DataType = *config.DataType
	}

	if config.Format != nil {
		c.Format = *config.Format
	}
}

//Columns wrap slice of Column
type Columns []*Column

//ColumnIndex represents *Column registry.
type ColumnIndex map[string]*Column

func (c ColumnIndex) ColumnName(key string) (string, error) {
	lookup, err := c.Lookup(key)
	if err != nil {
		return "", err
	}

	return lookup.Name, nil
}

//Index indexes columns by Column.Name
func (c Columns) Index(caser format.Case) ColumnIndex {
	result := ColumnIndex{}
	for i, _ := range c {
		result.Register(caser, c[i])
	}
	return result
}

//Register registers *Column
func (c ColumnIndex) Register(caser format.Case, column *Column) {
	keys := shared.KeysOf(column.Name, true)
	for _, key := range keys {
		c[key] = column
	}
	c[caser.Format(column.Name, format.CaseUpperCamel)] = column

	if column.field != nil {
		c[column.field.Name] = column
	}
}

//RegisterHolder looks for the Column by Relation.Column name.
//If it finds registers that Column with Relation.Holder key.
func (c ColumnIndex) RegisterHolder(relation *Relation) error {
	column, err := c.Lookup(relation.Column)
	if err != nil {
		//TODO: evaluate later
		return nil
	}

	c[relation.Holder] = column
	keys := shared.KeysOf(relation.Holder, false)
	for _, key := range keys {
		c[key] = column
	}

	return nil
}

//Lookup returns Column with given name.
func (c ColumnIndex) Lookup(name string) (*Column, error) {
	column, ok := c[name]
	if ok {
		return column, nil
	}

	column, ok = c[strings.ToUpper(name)]
	if ok {
		return column, nil
	}

	column, ok = c[strings.ToLower(name)]
	if ok {
		return column, nil
	}

	keys := make([]string, len(c))
	counter := 0
	for k := range c {
		keys[counter] = k
		counter++
	}

	return nil, fmt.Errorf("undefined column name %v, avails: %+v", name, strings.Join(keys, ","))
}

func (c ColumnIndex) RegisterWithName(name string, column *Column) {
	keys := shared.KeysOf(name, true)
	for _, key := range keys {
		c[key] = column
	}
}

//Init initializes each Column in the slice.
func (c Columns) Init(resource *Resource, config map[string]*ColumnConfig, caser format.Case, allowNulls bool) error {
	for i := range c {
		columnConfig := config[c[i].Name]

		if err := c[i].Init(resource, caser, allowNulls, columnConfig); err != nil {
			return err
		}
	}

	return nil
}

func (c Columns) updateTypes(columns []*Column, caser format.Case) {
	index := Columns(columns).Index(caser)

	for _, column := range c {
		if column.rType == nil || shared.Elem(column.rType).Kind() == reflect.Interface {
			newCol, err := index.Lookup(column.Name)
			if err != nil {
				continue
			}

			column.rType = newCol.rType
		}
	}
}

type ColumnConfig struct {
	Name       string  `json:",omitempty"`
	Expression *string `json:",omitempty"`
	Codec      *Codec  `json:",omitempty"`
	DataType   *string `json:",omitempty"`
	Format     *string `json:",omitempty"`
}
