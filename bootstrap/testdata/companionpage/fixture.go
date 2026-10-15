package companionpage

import (
	"github.com/viant/xdatly"
	legacy "github.com/viant/xdatly/handler/response"
)

type Input struct {
	ID  int       `parameter:"ID,kind=query,in=id,required=false"`
	Has *InputHas `setMarker:"true" json:"-"`
}
type InputHas struct{ ID bool }
type Output struct {
	legacy.Status `parameter:"Status,kind=output,in=status"`
	Data          []*Row `parameter:"Data,kind=output,in=view" view:"rows,type=Row,limit=2" sql:"SELECT r.* FROM (SELECT id,label FROM records WHERE id > $ID) r" json:"data"`
}
type Row struct {
	ID    int    `sqlx:"id" json:"id"`
	Label string `sqlx:"label" json:"label"`
}
type selectors struct {
	Fields []string `parameter:"Fields,kind=query,in=fields,required=false" querySelector:"view=rows,property=fields"`
	Page   int      `parameter:"Page,kind=query,in=page,required=false" querySelector:"view=rows,property=page"`
}
type Component struct {
	Contract xdatly.Component[Input, Output] `component:"ReadPage,path=/page-records,method=GET" ignoreEmptyQueryParameters:"false"`
}

func (Component) DatlyQuerySelectors() selectors { panic("metadata method must not be invoked") }

var Linked = new(Component)
