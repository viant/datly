package io

import (
	"fmt"
	"github.com/viant/datly/metadata"
	"reflect"
)

type Container interface {
	//New creates container item element
	New() interface{}
	//Add adds item to container
	Add(item interface{})
	//Range iterates container elements
	Range(fn func(item interface{}) error) error
	//Iter iterates containers element or nil if no more elements
	Iter() func() interface{}
}

type structContainer struct {
	componentType reflect.Type
	slicePtr      reflect.Value
	item          *reflect.Value
}

//New create an item value
func (c *structContainer) New() interface{} {
	var item reflect.Value
	if c.item != nil {
		item = *c.item
		c.item = nil
	} else {
		if c.componentType.Kind() == reflect.Ptr {
			item = reflect.New(c.componentType.Elem())
		} else {
			item = reflect.New(c.componentType)
		}
	}

	return item.Interface()
}

//Range iterates container items
func (c *structContainer) Range(fn func(item interface{}) error) error {
	aSlice := c.slicePtr.Elem()
	size := aSlice.Len()
	for i := 0; i < size; i++ {
		if err := fn(aSlice.Index(i).Addr().Interface()); err != nil {
			return err
		}
	}
	return nil
}

//Add add item to container
func (c *structContainer) Add(item interface{}) {
	itemValue := reflect.ValueOf(item)
	if c.componentType.Kind() == reflect.Struct {
		newSlice := reflect.Append(c.slicePtr.Elem(), itemValue.Elem())
		c.slicePtr.Elem().Set(newSlice)
		return
	}
	newSlice := reflect.Append(c.slicePtr.Elem(), itemValue)
	c.slicePtr.Elem().Set(newSlice)
}

func (c *structContainer) Iter() func() interface{} {
	size := c.slicePtr.Elem().Len()
	aSlice := c.slicePtr.Elem()
	i := 0
	return func() interface{} {
		if i < size {
			result := aSlice.Index(i).Addr().Interface()
			i++
			return result
		}
		return nil
	}
}

//NewStructContainer returns a struct container
func NewStructContainer(source interface{}) (Container, error) {
	sourceVal := reflect.ValueOf(source)
	if sourceVal.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("expected *Ptr, but had: %T", source)
	}
	var result = &structContainer{}
	switch sourceVal.Elem().Kind() {
	case reflect.Struct:
		result.componentType = sourceVal.Type()
		result.item = &sourceVal
		aSlice := reflect.MakeSlice(reflect.SliceOf(result.componentType), 0, 1)
		result.slicePtr = reflect.New(aSlice.Type())
		result.slicePtr.Elem().Set(aSlice)

	case reflect.Slice:
		result.componentType = sourceVal.Type().Elem().Elem()
		result.slicePtr = sourceVal
	}
	return result, nil
}

type genericContainer struct {
	componentType reflect.Type
	ptr           *[]interface{}
}

//Add add item to container
func (c *genericContainer) Add(item interface{}) {
	*c.ptr = append(*c.ptr, item)
}

//Range iterates container items
func (c *genericContainer) Range(fn func(item interface{}) error) error {
	for i := range *c.ptr {
		if err := fn((*c.ptr)[i]); err != nil {
			return err
		}
	}
	return nil
}

func (c *genericContainer) Iter() func() interface{} {
	size := len(*c.ptr)
	i := 0
	return func() interface{} {
		if i < size {
			result := (*c.ptr)[i]
			i++
			return result
		}
		return nil
	}
}

//New create an item value
func (c *genericContainer) New() interface{} {
	var item reflect.Value
	if c.componentType.Kind() == reflect.Ptr {
		item = reflect.New(c.componentType.Elem())
	} else {
		item = reflect.New(c.componentType)
	}
	return item.Interface()
}

//NewGenericContainer returns an interface container
func NewGenericContainer(target *[]interface{}, aType reflect.Type) (Container, error) {
	if aType == nil {
		return nil, fmt.Errorf("reflect type can not be empty for generic container")
	}
	return &genericContainer{
		ptr:           target,
		componentType: aType,
	}, nil
}

//NewContainer creates a container for target and a view, target has to be a pointer to struct or slice
//or implement Container interface
func NewContainer(target interface{}, aView *metadata.View) (Container, error) {
	var err error
	container, ok := target.(Container)
	if !ok {
		if genericSlice, ok := target.(*[]interface{}); ok {
			container, err = NewGenericContainer(genericSlice, aView.ReflectType())
			return container, err
		}
		container, err = NewStructContainer(target)
	}
	return container, err
}
