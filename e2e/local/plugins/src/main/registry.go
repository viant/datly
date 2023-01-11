package main

import (
	"github.com/viant/xdatly"
	"reflect"
)

var Config = &xdatly.Registry{
	Types: map[string]reflect.Type{
		"FooPlugin": reflect.TypeOf(&FooPlugin{}),
	},
}
