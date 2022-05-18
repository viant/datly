package view

import (
	"fmt"
	"reflect"
)

//Types represents reflect.Type registry
//map key should match Schema.Name
type Types map[string]reflect.Type

//Register registers Type
func (r Types) Register(name string, rType reflect.Type) {
	r[name] = rType
}

//Lookup returns Type
func (r Types) Lookup(name string) (reflect.Type, error) {
	rType, ok := r[name]
	if !ok {
		return nil, fmt.Errorf("not found type %v", name)
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
