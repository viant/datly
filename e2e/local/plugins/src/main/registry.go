package main

import (
	"github.com/viant/datly/plugins"
	"reflect"
)

var Config = &plugins.Registry{
	Types: map[string]reflect.Type{
		"FooPlugin": reflect.TypeOf(FooPlugin{}),
	},
}
