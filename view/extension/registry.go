package extension

import (
	"fmt"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/docs"
	"github.com/viant/xdatly/types/core"
	"github.com/viant/xreflect"
	"reflect"
	"sync"
	"time"
)

type Registry struct {
	sync.Mutex
	Types      *xreflect.Types
	Codecs     *codec.Registry
	Predicates *PredicateRegistry
	Docs       *docs.Registry
}

func NewRegistry() *Registry {
	types := Config.Types

	if pkgTypes, _ := core.Types(func(packageName, typeName string, rType reflect.Type, insertedAt time.Time) {
		fmt.Printf("loaded types: %v %v\n", types, rType)
		_ = types.Register(typeName, xreflect.WithPackage(packageName), xreflect.WithReflectType(rType))
	}); len(pkgTypes) > 0 {
		for pkg, v := range pkgTypes {
			for name, v := range v {
				_ = types.Register(name, xreflect.WithPackage(pkg), xreflect.WithReflectType(v))
			}
		}
	}

	predictes := Config.Predicates
	return &Registry{
		Mutex:      sync.Mutex{},
		Types:      xreflect.NewTypes(xreflect.WithRegistry(Config.Types)),
		Codecs:     Config.Codecs,
		Predicates: predictes,
	}
}

func (r *Registry) LookupCodec(name string) (*codec.Codec, error) {
	r.Lock()
	defer r.Unlock()
	return r.Codecs.Lookup(name)
}

func (r *Registry) RegisterCodec(name string, codecInstance codec.Instance, at time.Time) {
	r.Lock()
	defer r.Unlock()

	r.Codecs.RegisterInstance(name, codecInstance, at)
}

func (r *Registry) MergeFrom(toOverride *Registry) {
	r.Lock()
	defer r.Unlock()
	_ = r.Types.MergeFrom(toOverride.Types)

	codecs, _ := toOverride.Codecs.Codecs(nil)
	for key, value := range codecs {
		r.Codecs.RegisterCodec(key, value)
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
