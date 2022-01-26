package data

import "reflect"

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
	res := &Component{
		compType: cType,
	}
	res.Init()
	return res
}

//ComponentType returns struct type built based on Fields
func (c *Component) ComponentType() reflect.Type {
	if c.compType == nil {
		c.Init()
	}

	return c.compType
}

//Init build struct type from Fields
func (c *Component) Init() {
	if c.compType != nil && len(c.Fields) == 0 {
		c.Fields = make([]Field, c.compType.NumField())
		for i := range c.Fields {
			field := c.compType.Field(i)
			c.Fields[i] = Field{
				Name:      field.Name,
				DataType:  field.Type.Name(),
				fieldType: field.Type,
				Tag:       field.Tag,
			}
		}
	}
}
