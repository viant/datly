package csv

import (
	"github.com/viant/datly/converter"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type Object struct {
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

func (o *Object) Build() error {
	_, err := o.build()
	return err
}

func (o *Object) build() (interface{}, error) {
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

	if err := o.buildChildren(xunsafe.AsPointer(value), o.children); err != nil {
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

func (o *Object) buildChildren(parent unsafe.Pointer, children []*Object) error {
	for _, child := range children {
		childValue, err := child.build()
		if err != nil {
			return err
		}

		if child.Has(parent, childValue) {
			continue
		}

		child.merge(parent, childValue)
		if err = child.buildChildren(xunsafe.AsPointer(childValue), child.children); err != nil {
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
