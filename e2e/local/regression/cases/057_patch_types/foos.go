package main

import (
	"reflect"
)

var PackageName = "foos"

var Types map[string]reflect.Type

func init() {
	Types = map[string]reflect.Type{
		"FoosPerformance": reflect.TypeOf(FoosPerformance{}),
		"Foos":            reflect.TypeOf(Foos{}),
	}

}

type Foos struct {
	Id              int                `sqlx:"name=ID,autoincrement,primaryKey"`
	Name            *string            `sqlx:"name=NAME" json:",omitempty" validate:"omitempty,le(255)"`
	Quantity        *int               `sqlx:"name=QUANTITY" json:",omitempty"`
	FoosPerformance []*FoosPerformance `typeName:"FoosPerformance" sqlx:"-" datly:"relName=FoosPerformance,relColumn=ID,relField=Id,refTable=FOOS_PERFORMANCE,refColumn=FOO_ID,refField=FooId" sql:"SELECT * FROM FOOS_PERFORMANCE"`
	Has             *FoosHas           `setMarker:"true" typeName:"FoosHas" json:"-"  sqlx:"-" `
}

type FoosPerformance struct {
	Id           int                 `sqlx:"name=ID,autoincrement,primaryKey"`
	PerfName     *string             `sqlx:"name=PERF_NAME" json:",omitempty" validate:"omitempty,le(255)"`
	PerfQuantity *int                `sqlx:"name=PERF_QUANTITY" json:",omitempty"`
	FooId        *int                `sqlx:"name=FOO_ID,refTable=FOOS,refColumn=ID" json:",omitempty"`
	Has          *FoosPerformanceHas `setMarker:"true" typeName:"FoosPerformanceHas" json:"-"  sqlx:"-" `
}

type FoosPerformanceHas struct {
	Id           bool
	PerfName     bool
	PerfQuantity bool
	FooId        bool
}

type FoosHas struct {
	Id              bool
	Name            bool
	Quantity        bool
	FoosPerformance bool
}
