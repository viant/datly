package selectorprefixed

import "github.com/viant/xdatly"

type Input struct {
	ID  int       `parameter:"ID,kind=query,in=id,required=false"`
	Has *InputHas `setMarker:"true" json:"-"`
}
type InputHas struct{ ID bool }
type Output struct {
	Other []*Row `parameter:"Other,kind=output,in=derived" view:"other,type=Row" sql:"SELECT id,label FROM extras"`
	Data  []*Row `parameter:"Data,kind=output,in=view" view:"rows,type=Row,limit=2" sql:"SELECT id,label FROM records"`
}
type Row struct {
	ID    int    `sqlx:"id"`
	Label string `sqlx:"label"`
}
type selectors struct {
	OtherFields []string `parameter:"Fields,kind=query,in=other_fields,required=false" querySelector:"view=other,property=fields"`
	OtherPage   int      `parameter:"Page,kind=query,in=other_page,required=false" querySelector:"view=other,property=page"`
	DeFields    []string `parameter:"Fields,kind=query,in=de_fields,required=false" querySelector:"view=rows,property=fields"`
	DePage      int      `parameter:"Page,kind=query,in=de_page,required=false" querySelector:"view=rows,property=page"`
	DeLimit     int      `parameter:"Limit,kind=query,in=de_limit,required=false" querySelector:"view=rows,property=limit"`
	DeOffset    int      `parameter:"Offset,kind=query,in=de_offset,required=false" querySelector:"view=rows,property=offset"`
	DeOrderby   []string `parameter:"OrderBy,kind=query,in=de_orderby,required=false" querySelector:"view=rows,property=orderBy"`
}
type Component struct {
	Contract xdatly.Component[Input, Output] `component:"ReadRecords,path=/selector-records,method=GET"`
}

func (Component) DatlyQuerySelectors() selectors { panic("metadata method must not be invoked") }

var Linked = new(Component)
