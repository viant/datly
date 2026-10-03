package childwarmup

import (
	"reflect"

	"github.com/viant/xdatly"
)

type Components struct {
	Read xdatly.Component[Input, Output]      `component:"Parent,path=/child-warmup,method=GET,connector=main,view=parents" apiKeyHeader:"X-Read" apiKeyValue:"read-key"`
	Lazy xdatly.Component[Input, PlainOutput] `component:"Unrelated,path=/unrelated,method=GET,connector=main,view=plain"`
}

var LinkedType = reflect.TypeFor[Components]()

type Input struct{}
type Output struct {
	Rows []*Parent `parameter:"Rows,kind=output,in=view" view:"parents,table=records" json:"rows"`
}
type Parent struct {
	ID       int       `sqlx:"id" json:"id"`
	Name     string    `sqlx:"name" json:"name"`
	Timeline []*Metric `view:"timeline,table=warm_timeline,cache=shared,cacheWarmup=timelineWarmup" on:"ID:id=ParentID:parent_id" json:"timeline"`
	Summary  []*Metric `view:"summary,table=warm_summary,cache=shared,cacheWarmup=summaryWarmup" on:"ID:id=ParentID:parent_id" json:"summary"`
}
type Metric struct {
	ParentID int `sqlx:"parent_id" json:"parentId"`
	Value    int `sqlx:"value" json:"value"`
}
type PlainOutput struct {
	Rows []*Plain `parameter:"Rows,kind=output,in=view" view:"plain,table=records" json:"rows"`
}
type Plain struct {
	ID int `sqlx:"id" json:"id"`
}
