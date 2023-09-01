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
	c.types[t.Schema.rType] = t
	c.RWMutex.Unlock()
}

func NewTypes() *Types {
	return &Types{types: make(map[reflect.Type]*Type)}
}
