package selectorside

import "github.com/viant/xdatly"

type Input struct {
	ID  int       `parameter:"ID,kind=query,in=id,required=false"`
	Has *InputHas `setMarker:"true" json:"-"`
}
type InputHas struct{ ID bool }
type Output struct {
	Data []*Row `parameter:"Data,kind=output,in=view" view:"rows,type=Row,limit=2" sql:"SELECT id,label FROM records WHERE id > $ID"`
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
	Contract xdatly.Component[Input, Output] `component:"ReadRows,path=/side-records,method=GET"`
}

func (Component) DatlyQuerySelectors() selectors { panic("declaration must never execute") }

var Linked = new(Component)

type pageSelectors struct {
	Page int `parameter:"Page,kind=query,in=page,required=false" querySelector:"view=rows,property=page"`
}
type PageComponent struct {
	Contract xdatly.Component[Input, Output] `component:"ReadPageOnly,path=/side-page-records,method=GET"`
}

func (PageComponent) DatlyQuerySelectors() pageSelectors { panic("declaration must never execute") }

var PageLinked = new(PageComponent)
