package visitor

import (
	"github.com/pkg/errors"
	"sync"
)

//ServiceRegistry represents hook registry
type ServiceRegistry interface {
	//Register registers visitor
	Register(name string, visitor Visit)
	//Get returns visitor
	Get(name string) (Visit, error)
	//Remove removes visitor
	Remove(name string)
}

//ServiceRegistry visitor hook registry
type registry struct {
	registry map[string]Visit
	mux      sync.RWMutex
}

//Register registers visitor
func (r *registry) Register(name string, visitor Visit) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.registry[name] = visitor
}

//Get returns visitor
func (r *registry) Get(name string) (Visit, error) {
	r.mux.RLock()
	defer r.mux.RUnlock()
	result, ok := r.registry[name]
	if !ok {
		return nil, errors.Errorf("failed to lookup visitor: %v", name)
	}
	return result, nil
}

//Remove removes visitor
func (r *registry) Remove(name string) {
	r.mux.Lock()
	defer r.mux.Unlock()
	delete(r.registry, name)
}

//New creates a registry
func New() ServiceRegistry {
	return &registry{
		registry: make(map[string]Visit),
		mux:      sync.RWMutex{},
	}
}
