package version

import (
	"fmt"
	"github.com/viant/datly/view"
	"sync"
)

type (
	Resource struct {
		*view.Resource
		Control
	}

	Resources struct {
		resources map[string]*Resource
		sync.RWMutex
	}
)

func (r *Resources) Put(name string, resource *Resource) {
	r.Lock()
	defer r.Unlock()
	if len(r.resources) == 0 {
		r.resources = map[string]*Resource{}
	}
	r.resources[name] = resource
}

func (r *Resources) Lookup(name string) (*Resource, error) {
	r.RLock()
	defer r.RUnlock()
	ret, ok := r.resources[name]
	if !ok {
		return nil, fmt.Errorf("failed to locate 'with' resource: %s", name)
	}
	return ret, nil
}
