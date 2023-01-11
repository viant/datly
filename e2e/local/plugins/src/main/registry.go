package main

import (
	"github.com/viant/datly/xdatly"
	"reflect"
)

var Config = &xdatly.Registry{
	Types: map[string]reflect.Type{
		"FooPlugin": reflect.TypeOf(&FooPlugin{}),
	},
}
