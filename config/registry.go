package config

import (
	"github.com/viant/xreflect"
	"reflect"
	"sync"
)

type Registry struct {
	sync.Mutex
	Types      *xreflect.Types
	Codecs     CodecsRegistry
	Predicates *PredicateRegistry
}

func NewRegistry() *Registry {
	return &Registry{
		Mutex:      sync.Mutex{},
		Types:      xreflect.NewTypes(xreflect.WithRegistry(Config.Types)),
		Codecs:     CodecsRegistry{},
		Predicates: NewPredicates(),
	}
}

func (r *Registry) LookupCodec(name string) (BasicCodec, error) {
	r.Lock()
	defer r.Unlock()
	return r.Codecs.LookupCodec(name)
}

func (r *Registry) RegisterCodec(visitor BasicCodec) {
	r.Lock()
	defer r.Unlock()

	r.Codecs.Register(visitor)
}

func (r *Registry) MergeFrom(toOverride *Registry) {
	r.Lock()
	defer r.Unlock()
	_ = r.Types.MergeFrom(toOverride.Types)
	for name, codec := range toOverride.Codecs {
		r.Codecs.RegisterWithName(name, codec)
	}
}

func (r *Registry) setType(types map[string]reflect.Type, name string, rType reflect.Type) {
	types[name] = rType
}

func (r *Registry) AddTypes(pkgName string, types []reflect.Type) {
	r.Lock()
	defer r.Unlock()
	for _, rType := range types {
		_ = r.Types.Register(rType.Name(), xreflect.WithPackage(pkgName), xreflect.WithReflectType(rType))
	}
}
