package tabjson

import (
	"fmt"
	"github.com/viant/sqlx/converter"
	"github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type Object struct {
	holder       string
	dest         interface{}
	appender     *xunsafe.Appender
	values       []*string
	objType      reflect.Type
	fields       []*Field
	path         string
	parentID     interface{}
	index        *Index
	uniqueFields []int // Supported only one field for now
	children     []*Object
	xField       *xunsafe.Field
	xSlice       *xunsafe.Slice
	stringifier  *io.ObjectStringifier
}

func (o *Object) ID() interface{} {
	return o.path
}

func (o *Object) ParentID() interface{} {
	return o.parentID
}

func (o *Object) AddChildren(node Node) {
	child, ok := node.(*Object)
	if !ok {
		return
	}

	o.children = append(o.children, child)
}

func (o *Object) AddHolder(field *Field, holder *string) {
	if field.unique {
		o.uniqueFields = []int{len(o.values)}
	}

	o.values = append(o.values, holder)
	o.fields = append(o.fields, field)
}

func (o *Object) Umarshal() error {
	_, err := o.unmarshal()
	return err
}

func (o *Object) unmarshal() (interface{}, error) {
	indexed, ok := o.CheckIndexed()
	if ok {
		return indexed, nil
	}

	value := reflect.New(o.objType).Elem().Interface()
	valuePtr := xunsafe.AsPointer(value)
	for i, field := range o.fields {
		converted, wasNil, err := converter.Convert(*o.values[i], field.xField.Type, "")
		if wasNil {
			continue
		}

		if err != nil {
			return nil, err
		}

		field.xField.SetValue(valuePtr, converted)
	}

	if err := o.unmarshalChildren(xunsafe.AsPointer(value), o.children); err != nil {
		return nil, err
	}

	o.appender.Append(value)
	return value, nil
}

func (o *Object) CheckIndexed() (interface{}, bool) {
	if len(o.uniqueFields) == 0 {
		return nil, false
	}

	for _, fieldIndex := range o.uniqueFields {
		value, ok := o.index.Get(*o.values[fieldIndex])
		if ok {
			return value, ok
		}
	}

	return nil, false
}

func (o *Object) unmarshalChildren(parent unsafe.Pointer, children []*Object) error {
	for _, child := range children {
		childValue, err := child.unmarshal()
		if err != nil {
			return err
		}

		if child.Has(parent, childValue) {
			continue
		}

		child.merge(parent, childValue)
		if err = child.unmarshalChildren(xunsafe.AsPointer(childValue), child.children); err != nil {
			return err
		}
	}

	return nil
}

func (o *Object) Has(parent interface{}, value interface{}) bool {
	return o.index.Has(parent, value)
}

func (o *Object) merge(parent unsafe.Pointer, value interface{}) {
	if o.xSlice != nil {
		o.objectAppender(parent).Append(value)
		return
	}

	o.xField.SetValue(parent, value)
}

func (o *Object) objectAppender(parent unsafe.Pointer) *xunsafe.Appender {
	appender, ok := o.index.appenders[parent]
	if !ok {
		appender = o.xSlice.Appender(o.xField.ValuePointer(parent))
		o.index.appenders[parent] = appender
	}

	return appender
}

func (o *Object) Accessor(accessorIndex int, mainConfig *Config, depth int, configs []*Config) (*Accessor, error) {
	children := make([]*Accessor, 0, len(o.children))
	for i, child := range o.children {
		childAccessor, err := child.Accessor(i, mainConfig, depth+1, configs)
		if err != nil {
			return nil, err
		}

		children = append(children, childAccessor)
	}

	config, err := o.depthConfig(configs, mainConfig, depth)
	if err != nil {
		return nil, err
	}

	accessor := &Accessor{
		cache:               map[unsafe.Pointer]*stringified{},
		path:                o.path,
		config:              config,
		fields:              o.fields,
		field:               o.xField,
		children:            children,
		slice:               o.xSlice,
		parentAccessorIndex: accessorIndex,
		holder:              o.holder,
	}

	for _, child := range children {
		child._parent = accessor
	}

	return accessor, nil
}

func (o *Object) depthConfig(configs []*Config, mainConfig *Config, depth int) (*Config, error) {
	if depth == 0 {
		return mainConfig, nil
	}

	if len(configs) == 0 {
		return nil, nil
	}

	if len(configs) > depth-1 {
		return configs[depth-1], nil
	}

	return nil, fmt.Errorf("not specified config for the %v depth", depth-1)
}
