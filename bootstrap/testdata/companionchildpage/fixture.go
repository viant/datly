package companionchildpage

import "github.com/viant/xdatly"

type Input struct {
	ID int `parameter:"ID,kind=query,in=id,required=false"`
}
type Output struct {
	Data []*Row `parameter:"Data,kind=output,in=view" view:"rows,type=Row,limit=2" sql:"SELECT r.* FROM (SELECT id,label FROM records) r" json:"data"`
}
type Row struct {
	ID       int      `sqlx:"id" json:"id"`
	Label    string   `sqlx:"label" json:"label"`
	Children []*Child `json:"children" on:"ID:rows.id=ID:children.id" view:"children,type=Child,limit=1" sql:"SELECT children.id,children.label FROM records children"`
}
type Child struct {
	ID    int    `sqlx:"id" json:"id"`
	Label string `sqlx:"label" json:"label"`
}
type selectors struct {
	Page       int `parameter:"Page,kind=query,in=page,required=false" querySelector:"view=rows,property=page"`
	ChildPage  int `parameter:"ChildPage,kind=query,in=childPage,required=false" querySelector:"view=children,property=page"`
	ChildLimit int `parameter:"ChildLimit,kind=query,in=childLimit,required=false" querySelector:"view=children,property=limit"`
}
type Component struct {
	Contract xdatly.Component[Input, Output] `component:"ReadNestedPage,path=/nested-page,method=GET" ignoreEmptyQueryParameters:"false"`
}

func (Component) DatlyQuerySelectors() selectors { panic("metadata method must not be invoked") }

var Linked = new(Component)
