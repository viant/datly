package main

import (
	"reflect"
)

var PackageName = "events"

var Types = map[string]reflect.Type{
	"Data": reflect.TypeOf(Data{}),
}

type Data struct {
	Data *Events `typeName:"Events"`
}

type Events struct {
	Id       int        `sqlx:"name=ID,autoincrement,primaryKey"`
	Name     string     `sqlx:"name=NAME" json:",omitempty"`
	Quantity int        `sqlx:"name=QUANTITY" json:",omitempty"`
	Has      *EventsHas `setMarker:"true" typeName:"EventsHas" json:"-" sqlx:"-"`
}

type EventsHas struct {
	Id       bool
	Name     bool
	Quantity bool
}
