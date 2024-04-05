package state

import (
	"reflect"
	"sync"
)

type Types struct {
	types map[reflect.Type]*Type
	sync.RWMutex
}

func (c *Types) Lookup(p reflect.Type) (*Type, bool) {
	c.RWMutex.RLock()
	ret, ok := c.types[p]
	c.RWMutex.RUnlock()
	return ret, ok
}

func (c *Types) Put(t *Type) {
	c.RWMutex.Lock()
	rType := t.Schema.rType
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	c.types[rType] = t
	c.RWMutex.Unlock()
}

func NewTypes() *Types {
	return &Types{types: make(map[reflect.Type]*Type)}
}
