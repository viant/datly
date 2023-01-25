package plugins

import (
	"reflect"
	"sync"
)

type Registry struct {
	sync.Mutex
	Types  map[string]reflect.Type
	Codecs CodecsRegistry
}

func (r *Registry) LookupCodec(name string) (BasicCodec, error) {
	return r.Codecs.Lookup(name)
}

func (r *Registry) RegisterCodec(visitor BasicCodec) {
	r.Codecs.Register(visitor)
}

func (r *Registry) Override(toOverride *Registry) {
	r.Lock()
	defer r.Unlock()

	for typeName, rType := range toOverride.Types {
		r.Types[typeName] = rType
	}

	for _, codec := range toOverride.Codecs {
		r.Codecs.Register(codec)
	}
}
