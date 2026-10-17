package derivedselector

import "github.com/viant/xdatly"

type Input struct{}
type Output struct {
	Data []*Row `parameter:"Data,kind=output,in=view" view:"rows,type=Row,limit=2" sql:"SELECT r.* FROM (SELECT id,label FROM records) r" json:"data"`
}
type Row struct {
	ID    int    `sqlx:"id" json:"id"`
	Label string `sqlx:"label" json:"label"`
	Total *int   `sqlx:"-" json:"computed-total"`
}
type selectors struct {
	Fields []string `parameter:"Fields,kind=query,in=fields,required=false" querySelector:"view=rows,property=fields"`
	Page   int      `parameter:"Page,kind=query,in=page,required=false" querySelector:"view=rows,property=page"`
}
type Component struct {
	Contract xdatly.Component[Input, Output] `component:"DerivedRecords,path=/derived-records,method=GET" ignoreEmptyQueryParameters:"false"`
}

func (Component) DatlyQuerySelectors() selectors { panic("metadata method must not be invoked") }

var Linked = new(Component)
