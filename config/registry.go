package config

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
		Codecs:   CodecsRegistry{},
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

func (r *Registry) Override(toOverride *Registry) {
	r.Lock()
	defer r.Unlock()

	for typeName, rType := range toOverride.Types {
		r.setType(r.Types, typeName, rType)
	}

	r.overridePackageNamedTypes(toOverride.Packages)

	for name, codec := range toOverride.Codecs {
		r.Codecs.RegisterWithName(name, codec)
	}
}

func (r *Registry) OverrideTypes(packageName string, types map[string]reflect.Type) {
	r.Lock()
	defer r.Unlock()

	typesRegistry := r.getTypesRegsitry(packageName)
	for name, rType := range types {
		r.setType(typesRegistry, name, rType)
	}
}

func (r *Registry) getTypesRegsitry(packageName string) map[string]reflect.Type {
	typesRegistry := r.Types
	if packageName != "" {
		typesRegistry = r.packageRegistry(packageName)
	}

	return typesRegistry
}

func (r *Registry) setType(types map[string]reflect.Type, name string, rType reflect.Type) {
	types[name] = rType
}

func (r *Registry) AddTypes(name string, types []reflect.Type) {
	r.Lock()
	defer r.Unlock()

	regsitry := r.getTypesRegsitry(name)
	for _, rType := range types {
		r.setType(regsitry, rType.Name(), rType)
	}
}

func (r *Registry) OverridePackageTypes(packageTypes map[string][]reflect.Type) {
	r.Lock()
	defer r.Unlock()

	for packageName, types := range packageTypes {
		registry := r.packageRegistry(packageName)

		for _, rType := range types {
			typeName := rType.Name()
			r.setType(registry, typeName, rType)
		}
	}
}

func (r *Registry) PackageRegistry(name string) map[string]reflect.Type {
	r.Lock()
	defer r.Unlock()
	return r.packageRegistry(name)
}

func (r *Registry) packageRegistry(name string) map[string]reflect.Type {
	registry, ok := r.Packages[name]
	if ok {
		return registry
	}

	registry = map[string]reflect.Type{}
	r.Packages[name] = registry
	return registry
}

func (r *Registry) OverridePackageNamedTypes(packageTypes map[string]map[string]reflect.Type) {
	r.Lock()
	defer r.Unlock()

	r.overridePackageNamedTypes(packageTypes)
}

func (r *Registry) overridePackageNamedTypes(packageTypes map[string]map[string]reflect.Type) {
	for packageName, types := range packageTypes {
		registry := r.packageRegistry(packageName)

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

func (r *Registry) AddType(packageName string, typeName string, rType reflect.Type) {
	r.Lock()
	defer r.Unlock()

	regsitry := r.getTypesRegsitry(packageName)
	regsitry[typeName] = rType
}
