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
	defer c.RWMutex.RUnlock()
	if len(c.types) == 0 {
		return nil, false
	}
	ret, ok := c.types[p]
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
