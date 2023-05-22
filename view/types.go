package view

import (
	"fmt"
	"reflect"
)

//Types represents reflect.Type registry
//map key should match Schema.Name / Schema.DataType
type Types map[string]reflect.Type

//Register registers Type
func (r Types) Register(name string, rType reflect.Type) {
	r[name] = rType
}

//Lookup returns Type
func (r Types) Lookup(name string) (reflect.Type, error) {
	rType, ok := r[name]
	if !ok {
		return nil, fmt.Errorf("not found type %v at Types", name)
	}

	return rType, nil
}

func (r Types) copy() Types {
	rCopy := Types{}

	for key := range r {
		rCopy[key] = r[key]
	}

	return rCopy
}

func (r Types) LookupType(path string, identifier string, name string) (reflect.Type, error) {
	lookup, err := r.Lookup(name)
	return lookup, err
}

func isSlice(fType reflect.Type) bool {
	if fType.Kind() == reflect.Ptr {
		fType = fType.Elem()
	}
	return fType.Kind() == reflect.Slice
}

func getStruct(fType reflect.Type) reflect.Type {
	switch fType.Kind() {
	case reflect.Ptr:
		return getStruct(fType.Elem())
	case reflect.Slice:
		return getStruct(fType.Elem())
	case reflect.Struct:
		return fType
	}
	return nil
}

//PackagedType represtns a package type
type PackagedType struct {
	Package string
	Name    string
	reflect.Type
}

func NewPackagedType(pkg string, name string, t reflect.Type) *PackagedType {
	return &PackagedType{Package: pkg, Name: name, Type: t}
}
