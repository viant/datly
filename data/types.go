package data

import "reflect"

//Types represents reflect.Type registry
type Types map[string]reflect.Type

//Register registers Type
func (r Types) Register(name string, rType reflect.Type) {
	r[name] = rType
}

//Lookup returns Type
func (r Types) Lookup(name string) reflect.Type {
	return r[name]
}
