package data

import "reflect"

type Types map[string]reflect.Type

func (r Types) Register(name string, p reflect.Type) {
	r[name] = p
}

func (r Types) Lookup(name string) reflect.Type {
	return r[name]
}
