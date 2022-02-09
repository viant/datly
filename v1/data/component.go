package data

import (
	"github.com/viant/sqlx/io"
	"reflect"
	"strings"
)

type (
	Component struct {
		Name     string
		Fields   []Field
		compType reflect.Type
	}

	Field struct {
		Name      string
		DataType  string
		fieldType reflect.Type
		Tag       reflect.StructTag
	}
)

//NewComponent creates Component instance
func NewComponent(cType reflect.Type) *Component {
	if cType.Kind() != reflect.Ptr {
		cType = reflect.New(cType).Type()
	}

	res := &Component{
		compType: cType,
	}
	res.Init()
	return res
}

//ComponentType returns struct type
func (c *Component) ComponentType() reflect.Type {
	if c.compType == nil {
		c.Init()
	}

	if c.compType.Kind() != reflect.Ptr {
		panic("component type has to be a pointer")
	}

	return c.compType
}

func (c *Component) setType(rType reflect.Type) {
	switch rType.Kind() {
	case reflect.Struct:
		rType = reflect.PtrTo(rType)
	}
	c.compType = rType
}

//Init build struct type from Fields
func (c *Component) Init() {
	if c.compType != nil && len(c.Fields) == 0 {
		elem := c.compType.Elem()
		c.Fields = make([]Field, elem.NumField())
		for i := range c.Fields {
			field := elem.Field(i)
			c.Fields[i] = Field{
				Name:      field.Name,
				DataType:  field.Type.Name(),
				fieldType: field.Type,
				Tag:       field.Tag,
			}
		}
	}
}

func (c *Component) ensureType(types []io.Column) {
	if c.compType != nil {
		return
	}

	fieldsLen := len(c.Fields)
	if fieldsLen != 0 {
		c.initType(types, fieldsLen)
		return
	}

	c.initTypeAndFields(types)
}

func (c *Component) initType(types []io.Column, fieldsLen int) {
	includedColumns := make(map[string]bool)

	for i := 0; i < fieldsLen; i++ {
		includedColumns[strings.Title(c.Fields[i].Name)] = true
	}

	structFields := make([]reflect.StructField, fieldsLen)
	counter := 0
	for i := 0; i < fieldsLen; i++ {
		structFieldName := strings.Title(types[i].Name())
		if _, ok := includedColumns[structFieldName]; !ok {
			continue
		}
		structFields[i] = reflect.StructField{
			Name:  structFieldName,
			Type:  types[i].ScanType(),
			Index: []int{counter},
			Tag:   `sqlx:"name="`,
		}

		counter++
	}

	c.setType(reflect.StructOf(structFields))
}

func (c *Component) initTypeAndFields(types []io.Column) {
	structFields := make([]reflect.StructField, len(types))
	fields := make([]Field, len(types))

	counter := 0
	for i := 0; i < len(types); i++ {
		structFieldName := strings.Title(types[i].Name())
		scanType := types[i].ScanType()
		structFields[counter] = reflect.StructField{
			Name:  structFieldName,
			Type:  scanType,
			Index: []int{counter},
		}

		fields[counter] = Field{
			Name:      structFieldName,
			DataType:  scanType.Name(),
			fieldType: scanType,
			Tag:       "",
		}

		counter++
	}

	c.setType(reflect.StructOf(structFields))
	c.Fields = fields
}
