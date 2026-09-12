package shape

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/x"
)

// ResolveRootType resolves source root type from explicit Type, Struct, or viant/x registry.
func (s *Source) ResolveRootType() (reflect.Type, error) {
	if s == nil {
		return nil, ErrNilSource
	}
	if s.Type != nil {
		return unwrapPtr(s.Type), nil
	}
	if s.Struct != nil {
		return unwrapPtr(reflect.TypeOf(s.Struct)), nil
	}
	key := strings.TrimSpace(s.TypeName)
	if key == "" || s.TypeRegistry == nil {
		return nil, ErrNilSource
	}
	aType := s.TypeRegistry.Lookup(key)
	if aType == nil || aType.Type == nil {
		return nil, fmt.Errorf("shape source: type %q not found in registry", key)
	}
	return unwrapPtr(aType.Type), nil
}

// EnsureTypeRegistry returns source registry ensuring root type is registered when available.
func (s *Source) EnsureTypeRegistry() *x.Registry {
	if s == nil {
		return nil
	}
	if s.TypeRegistry == nil {
		s.TypeRegistry = x.NewRegistry()
	}
	if rType, err := s.ResolveRootType(); err == nil && rType != nil {
		t := x.NewType(rType)
		if strings.TrimSpace(s.TypeName) == "" {
			s.TypeName = t.Key()
		}
		s.TypeRegistry.Register(t)
	}
	return s.TypeRegistry
}

func unwrapPtr(rType reflect.Type) reflect.Type {
	for rType != nil && rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType
}
