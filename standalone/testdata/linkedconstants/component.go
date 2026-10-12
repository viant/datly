package linkedconstants

import (
	"reflect"

	"github.com/viant/xdatly"
)

type Component struct {
	Computed xdatly.Component[Input, ComputedOutput] `component:"Computed,path=/linked-constants/computed,method=GET,connector=main,view=records"`
	Bound    xdatly.Component[Input, BoundOutput]    `component:"Bound,path=/linked-constants/bound,method=GET,connector=main,view=records"`
	Unsafe   xdatly.Component[Input, UnsafeOutput]   `component:"Unsafe,path=/linked-constants/unsafe,method=GET,connector=main,view=records"`
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

// Core keeps SQL-backed implementation fields out of the producer JSON.
type Core struct {
	CleanupStatus string `sqlx:"cleanup_status" json:"-"`
	CreatedAtRaw  string `sqlx:"created_at_raw" json:"-"`
}
type ComputedRecord struct {
	Core
	ID       int    `sqlx:"id" json:"id"`
	ReadMode string `sqlx:"read_mode" json:"readMode"`
}
type ComputedOutput struct {
	Rows []*ComputedRecord `parameter:"Rows,kind=output,in=view" view:"records,table=records" sql:"SELECT r.id, $Constant AS read_mode, r.cleanup_status, r.created_at_raw FROM (SELECT id, LOWER(TRIM(COALESCE(name, ''))) AS cleanup_status, CAST(CURRENT_TIMESTAMP AS TEXT) AS created_at_raw FROM records) r" json:"rows"`
}
