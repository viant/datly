package context

import (
	"reflect"
	"sync"
)

type MultiMap struct {
	reflect.Type
	keyFn func(instance interface{}) interface{}
	Data  map[interface{}][]interface{}
	sync.Mutex
}

func (m *MultiMap) Add(item interface{}) {
	key := m.keyFn(item)
	m.Data[key] = append(m.Data[key], item)
}

func (m *MultiMap) Iter() func() interface{} {
	size := len(m.Data)
	var keys  []interface{}
	for k := range m.Data {
		keys = append(keys, k)
	}
	i := 0
	return func() interface{} {
		if i < size {
			result := m.Data[keys[i]]
			i++
			return result
		}
		return nil
	}
}



//Range iterates container items
func (m *MultiMap) Range(fn func(item interface{}) error) error {
	for _, v := range m.Data {
		if err := fn(v); err != nil {
			return err
		}
	}
	return nil
}

func (m *MultiMap) New() interface{} {
	return reflect.New(m.Type).Interface()
}

func NewMultiMap(aType reflect.Type, fn func(instance interface{}) interface{}) *MultiMap {
	return &MultiMap{
		Type:  aType,
		keyFn: fn,
		Data:  make(map[interface{}][]interface{}),
	}
}

type Map struct {
	reflect.Type
	keyFn func(instance interface{}) interface{}
	Data  map[interface{}]interface{}
	sync.Mutex
}

//Range iterates container items
func (c *Map) Range(fn func(item interface{}) error) error {
	for _, v := range c.Data {
		if err := fn(v); err != nil {
			return err
		}
	}
	return nil
}


func (m *Map) Iter() func() interface{} {
	size := len(m.Data)
	var keys  []interface{}
	for k := range m.Data {
		keys = append(keys, k)
	}
	i := 0
	return func() interface{} {
		if i < size {
			result := m.Data[keys[i]]
			i++
			return result
		}
		return nil
	}
}



func (i *Map) New() interface{} {
	return reflect.New(i.Type).Interface()
}

func (i *Map) Add(item interface{}) {
	key := i.keyFn(item)
	i.Data[key] = item
}

func NewMap(aType reflect.Type, fn func(fn interface{}) interface{}) *Map {
	return &Map{
		Type:  aType,
		keyFn: fn,
		Data:  make(map[interface{}]interface{}),
	}
}
