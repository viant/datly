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
	Id       int        `sqlx:"ID,autoincrement,primaryKey,required"`
	Name     *string    `sqlx:"NAME" json:",omitempty" validate:"required"`
	Quantity *int       `sqlx:"QUANTITY" json:",omitempty" validate:"required"`
	Has      *EventsHas `setMarker:"true" typeName:"EventsHas" json:"-" sqlx:"-"`
}

type EventsHas struct {
	Id       bool
	Name     bool
	Quantity bool
}
