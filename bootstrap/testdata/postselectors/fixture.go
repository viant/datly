package postselectors

import "github.com/viant/xdatly"

type Input struct {
	Min int       `parameter:"Min,kind=body,in=min,required=false"`
	ID  int       `parameter:"ID,kind=query,in=id,required=false"`
	Has *InputHas `setMarker:"true" json:"-"`
}
type InputHas struct{ ID, Min bool }
type Output struct {
	Data []*Row `parameter:"Data,kind=output,in=view" view:"rows,type=Row,limit=2" sql:"SELECT id,label FROM records WHERE id >= $ID AND id > $Min"`
}
type Row struct {
	ID    int    `sqlx:"id"`
	Label string `sqlx:"label"`
}
type selectors struct {
	Fields []string `parameter:"Fields,kind=query,in=fields,required=false" querySelector:"view=rows,property=fields"`
	Page   int      `parameter:"Page,kind=query,in=page,required=false" querySelector:"view=rows,property=page"`
	Limit  int      `parameter:"Limit,kind=query,in=limit,required=false" querySelector:"view=rows,property=limit"`
	Offset int      `parameter:"Offset,kind=query,in=offset,required=false" querySelector:"view=rows,property=offset"`
	Sort   []string `parameter:"OrderBy,kind=query,in=sort,required=false" querySelector:"view=rows,property=orderBy"`
}
type Component struct {
	Contract xdatly.Component[Input, Output] `component:"ReadPostRows,path=/post-records,method=POST"`
}

func (Component) DatlyQuerySelectors() selectors { panic("declaration must never execute") }

var Linked = new(Component)
