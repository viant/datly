package selectortransport

import "github.com/viant/xdatly"

type Input struct {
	ID  int       `parameter:"ID,kind=query,in=id,required=false"`
	Has *InputHas `setMarker:"true" json:"-"`
}
type InputHas struct{ ID bool }
type Output struct {
	Data []*Row `parameter:"Data,kind=output,in=view" view:"rows,type=Row,limit=2" sql:"SELECT id,label FROM records"`
}
type Row struct {
	ID    int    `sqlx:"id"`
	Label string `sqlx:"label"`
}
type selectors struct {
	Limit int `parameter:"Limit,kind=query,in=limit,required=false" querySelector:"view=rows,property=limit"`
	Page  int `parameter:"Page,kind=query,in=page,required=false" querySelector:"view=rows,property=page"`
}
type Component struct {
	Contract xdatly.Component[Input, Output] `component:"ReadRecords,path=/selector-records,method=GET,handler=HandleRecords"`
}

func (Component) DatlyQuerySelectors() selectors { panic("metadata method must not be invoked") }

var Linked = new(Component)
