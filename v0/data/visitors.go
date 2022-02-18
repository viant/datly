package data

import (
	"github.com/pkg/errors"
	"sync"
)

//Registry represents hook visitors
type Visitors interface {
	//Register registers visitor
	Register(name string, visitor Visit)
	//Get returns visitor
	Get(name string) (Visit, error)
	//Remove removes visitor
	Remove(name string)
}

var visitorRegistry Visitors

//Data returns visitor visitors singleton
func VisitorRegistry() Visitors {
	if visitorRegistry != nil {
		return visitorRegistry
	}
	visitorRegistry = newVisitorRegistry()
	return visitorRegistry
}

//Registry visitor hook visitors
type visitors struct {
	registry map[string]Visit
	mux      sync.RWMutex
}

//Register registers visitor
func (r *visitors) Register(name string, visitor Visit) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.registry[name] = visitor
}

//Get returns visitor
func (r *visitors) Get(name string) (Visit, error) {
	r.mux.RLock()
	defer r.mux.RUnlock()
	result, ok := r.registry[name]
	if !ok {
		return nil, errors.Errorf("failed to lookup visitor: %v", name)
	}
	return result, nil
}

//Remove removes visitor
func (r *visitors) Remove(name string) {
	r.mux.Lock()
	defer r.mux.Unlock()
	delete(r.registry, name)
}

//NewVisitors creates a visitors
func newVisitorRegistry() Visitors {
	return &visitors{
		registry: make(map[string]Visit),
		mux:      sync.RWMutex{},
	}
}
