package main

import (
	"reflect"
)

var PackageName = "foos"

var Types = map[string]reflect.Type{
	"Foos": reflect.TypeOf(Foos{}),
}

type Foos struct {
	Id              int                `sqlx:"name=ID,autoincrement,primaryKey,required"`
	Name            *string            `sqlx:"name=NAME" json:",omitempty"`
	Quantity        *int               `sqlx:"name=QUANTITY" json:",omitempty"`
	FoosPerformance []*FoosPerformance `typeName:"FoosPerformance" sqlx:"-"`
	Has             *FoosHas           `setMarker:"true" typeName:"FoosHas" json:"-" sqlx:"-"`
}

type FoosPerformance struct {
	Id           int                 `sqlx:"name=ID,autoincrement,primaryKey,required"`
	PerfName     *string             `sqlx:"name=PERF_NAME" json:",omitempty"`
	PerfQuantity *int                `sqlx:"name=PERF_QUANTITY" json:",omitempty"`
	FooId        *int                `sqlx:"name=FOO_ID,refTable=FOOS,refColumn=ID" json:",omitempty"`
	Has          *FoosPerformanceHas `setMarker:"true" typeName:"FoosPerformanceHas" json:"-" sqlx:"-"`
}

type FoosPerformanceHas struct {
	Id           bool
	PerfName     bool
	PerfQuantity bool
	FooId        bool
}

type FoosHas struct {
	Id       bool
	Name     bool
	Quantity bool
}
