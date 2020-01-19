package base

import (
	"datly/generic"
	"sync"
)

//Registry represents a registry
type Registry struct {
	Data map[string]generic.Collection `json:",omitempty"`
	mux  sync.Mutex
}

//Put add data key
func (r *Registry) Put(key string, value generic.Collection) {
	r.mux.Lock()
	defer r.mux.Unlock()
	if len(r.Data) == 0 {
		r.Data = make(map[string]generic.Collection)
	}
	r.Data[key] = value
}


//Returns Slice for provided key
func (r *Registry) Get(key string)  generic.Collection {
	r.mux.Lock()
	defer r.mux.Unlock()
	if len(r.Data) == 0 {
		return nil
	}
	return r.Data[key]
}