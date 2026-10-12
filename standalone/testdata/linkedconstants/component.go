package linkedconstants

import (
	"reflect"

	"github.com/viant/xdatly"
)

type Component struct {
	Bound  xdatly.Component[Input, BoundOutput]  `component:"Bound,path=/linked-constants/bound,method=GET,connector=main,view=records"`
	Unsafe xdatly.Component[Input, UnsafeOutput] `component:"Unsafe,path=/linked-constants/unsafe,method=GET,connector=main,view=records"`
}

var LinkedType = reflect.TypeFor[Component]()

type Input struct {
	Constant string `parameter:"Constant,kind=const,in=Constant"`
	Count    int    `parameter:"Count,kind=const,in=Count"`
}

type Record struct {
	ID    int    `sqlx:"id" json:"id"`
	Label string `sqlx:"label" json:"label"`
	Count int    `sqlx:"count" json:"count"`
}

type BoundOutput struct {
	Rows []*Record `parameter:"Rows,kind=output,in=view" view:"records,table=records" sql:"SELECT id, $Constant AS label, $Count AS count FROM records" json:"rows"`
}

type UnsafeOutput struct {
	Rows []*Record `parameter:"Rows,kind=output,in=view" view:"records,table=records" sql:"SELECT id, '$Unsafe.Constant' AS label, $Count AS count FROM records" json:"rows"`
}
