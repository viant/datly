package main

import (
	"fmt"
	"reflect"
)

var PackageName = "foos"
var Types = []reflect.Type{
	reflect.TypeOf(FooPlugin{}),
}

type FooPlugin struct {
	Id       int           `sqlx:"name=ID,primaryKey"`
	Name     *string       `sqlx:"name=NAME" json:",omitempty"`
	Quantity *int          `sqlx:"name=QUANTITY" json:",omitempty"`
	Has      *FooPluginHas `presenceIndex:"true" typeName:"FoosHas" json:"-" sqlx:"presence=true"`
}

type FooPluginHas struct {
	Id       bool
	Name     bool
	Quantity bool
}

func (p *FooPlugin) Validate() (string, error) {
	return "", p.validate()
}

func (p *FooPlugin) validate() error {
	if p.Quantity == nil || *p.Quantity < 0 {
		return fmt.Errorf("quantity can't be negative")
	}

	if p.Name == nil || *p.Name == "" {
		return fmt.Errorf("name can't be empty")
	}

	return nil
}
