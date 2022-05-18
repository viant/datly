package contract

import (
	"github.com/viant/gtly"
	"sync"
)

//Collections represents a registry
type Collections struct {
	Data map[string]gtly.Collection `json:",omitempty"`
	mux  sync.Mutex
}

//Put add view key
func (r *Collections) Put(key string, value gtly.Collection) {
	r.mux.Lock()
	defer r.mux.Unlock()
	if len(r.Data) == 0 {
		r.Data = make(map[string]gtly.Collection)
	}
	r.Data[key] = value
}

//Get returns a collection for provided key
func (r *Collections) Get(key string) gtly.Collection {
	r.mux.Lock()
	defer r.mux.Unlock()
	if len(r.Data) == 0 {
		return nil
	}
	return r.Data[key]
}

//NewData creates new view
func NewData() *Collections {
	return &Collections{
		Data: make(map[string]gtly.Collection),
		mux:  sync.Mutex{},
	}
}
