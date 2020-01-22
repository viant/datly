package cache

import (
	"github.com/pkg/errors"
	"sync"
)

//ServiceRegistry represents cache registry
type ServiceRegistry interface {
	//Register registers service
	Register(name string, service Service)
	//Get returns service
	Get(name string) (Service, error)
	//Remove removes service
	Remove(name string)
	//Returns register cache services
	Keys() []string
}

//ServiceRegistry service hook registry
type registry struct {
	registry map[string]Service
	mux      sync.RWMutex
}

//Register registers service
func (r *registry) Register(name string, service Service) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.registry[name] = service
}

//Keys returns registered services
func (r *registry) Keys() []string {
	r.mux.RLock()
	defer r.mux.RUnlock()
	var result = make([]string, 0)
	for k := range r.registry {
		result = append(result, k)
	}
	return result
}

//Get returns service
func (r *registry) Get(name string) (Service, error) {
	r.mux.RLock()
	defer r.mux.RUnlock()
	result, ok := r.registry[name]
	if !ok {
		return nil, errors.Errorf("filed to lookup cache service: %v", name)
	}
	return result, nil
}

//Remove removes service
func (r *registry) Remove(name string) {
	r.mux.Lock()
	defer r.mux.Unlock()
	delete(r.registry, name)
}

//New creates a registry
func New() ServiceRegistry {
	return &registry{
		registry: make(map[string]Service),
		mux:      sync.RWMutex{},
	}
}
