package session

import (
	"github.com/viant/datly/view/state"
	"sync"
)

type cache struct {
	values        map[string]interface{}
	parameterLock map[string]sync.Locker
	sync.RWMutex
}

func (c *cache) lookup(parameter *state.Parameter) (interface{}, bool) {
	c.RWMutex.RLock()
	ret, ok := c.values[c.key(parameter)]
	c.RWMutex.RUnlock()
	return ret, ok
}

func (c *cache) lockParameter(parameter *state.Parameter) sync.Locker {
	c.RWMutex.Lock()
	ret, ok := c.parameterLock[c.key(parameter)]
	if !ok {
		ret = &sync.Mutex{}
		c.parameterLock[c.key(parameter)] = ret
	}
	c.RWMutex.Unlock()
	return ret
}

func (c *cache) put(parameter *state.Parameter, value interface{}) {
	c.RWMutex.Lock()
	c.values[c.key(parameter)] = value
	c.RWMutex.Unlock()
}

func (c *cache) key(parameter *state.Parameter) string {
	ret := parameter.Name
	return ret
}

func newCache() *cache {
	return &cache{values: make(map[string]interface{}), parameterLock: make(map[string]sync.Locker)}
}
