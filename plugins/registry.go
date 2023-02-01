package plugins

import (
	"fmt"
	"reflect"
	"sync"
)

type Registry struct {
	sync.Mutex
	Types    map[string]reflect.Type
	Packages map[string]map[string]reflect.Type
	Codecs   CodecsRegistry
}

func NewRegistry() *Registry {
	return &Registry{
		Mutex:    sync.Mutex{},
		Types:    map[string]reflect.Type{},
		Packages: map[string]map[string]reflect.Type{},
		Codecs:   map[string]BasicCodec{},
	}
}

func (r *Registry) LookupCodec(name string) (BasicCodec, error) {
	return r.Codecs.Lookup(name)
}

func (r *Registry) RegisterCodec(visitor BasicCodec) {
	r.Codecs.Register(visitor)
}

func (r *Registry) Override(toOverride *Registry) {

	for typeName, rType := range toOverride.Types {
		r.setType(r.Types, typeName, rType)
	}

	r.OverridePackageNamedTypes(toOverride.Packages)

	r.Lock()
	defer r.Unlock()
	for _, codec := range toOverride.Codecs {
		r.Codecs.Register(codec)
	}
}

func (r *Registry) OverrideTypes(packageName string, types map[string]reflect.Type) {
	typesRegistry := r.getTypesRegsitry(packageName)
	for name, rType := range types {
		r.setType(typesRegistry, name, rType)
	}
}

func (r *Registry) getTypesRegsitry(packageName string) map[string]reflect.Type {
	typesRegistry := r.Types
	if packageName != "" {
		r.ensurePackages()
		typesRegistry = r.PackageRegistry(packageName)
	}

	return typesRegistry
}

func (r *Registry) setType(types map[string]reflect.Type, name string, rType reflect.Type) {
	r.Lock()
	defer r.Unlock()

	types[name] = rType
}

func (r *Registry) AddTypes(name string, types []reflect.Type) {
	regsitry := r.getTypesRegsitry(name)
	for _, rType := range types {
		r.setType(regsitry, rType.Name(), rType)
	}
}

func (r *Registry) OverridePackageTypes(packageTypes map[string][]reflect.Type) {
	r.ensurePackages()
	for packageName, types := range packageTypes {
		registry := r.PackageRegistry(packageName)

		for _, rType := range types {
			typeName := rType.Name()
			r.setType(registry, typeName, rType)
		}
	}
}

func (r *Registry) ensurePackages() {
	r.Lock()
	if r.Packages == nil {
		r.Packages = map[string]map[string]reflect.Type{}
	}
	r.Unlock()
}

func (r *Registry) PackageRegistry(name string) map[string]reflect.Type {
	r.ensurePackages()

	r.Lock()
	registry, ok := r.Packages[name]
	if ok {
		r.Unlock()
		return registry
	}

	registry = map[string]reflect.Type{}
	r.Packages[name] = registry
	r.Unlock()
	return registry
}

func (r *Registry) OverridePackageNamedTypes(packageTypes map[string]map[string]reflect.Type) {
	r.ensurePackages()
	for packageName, types := range packageTypes {
		registry := r.PackageRegistry(packageName)

		for name, rType := range types {
			r.setType(registry, name, rType)
		}
	}
}

func (r *Registry) LookupType(_, packageName string, typeName string) (reflect.Type, error) {
	registry := r.PackageRegistry(packageName)
	rType, ok := registry[typeName]
	if !ok {
		if packageName != "" {
			typeName = packageName + "." + typeName
		}

		return nil, fmt.Errorf("not found type %v", typeName)
	}

	return rType, nil
}
