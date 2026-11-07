package companionrootalias

import "github.com/viant/xdatly"

type Input struct {
	Ordinary string `parameter:"Ordinary,kind=query,in=ordinary,required=false"`
}
type Output struct {
	Data []*Row `parameter:"Data,kind=output,in=view" view:"rows,type=Row" sql:"SELECT id FROM records"`
}
type Row struct {
	ID       int      `sqlx:"id"`
	Children []*Child `on:"ID:rows.id=ParentID:children.parent_id" view:"children,type=Child" sql:"SELECT id,parent_id FROM children"`
}
type Child struct {
	ID       int `sqlx:"id"`
	ParentID int `sqlx:"parent_id"`
}
type selectors struct {
	Fields      []string `parameter:"Fields,kind=query,in=fields,required=false" querySelector:"view=rows,property=fields"`
	OrderBy     []string `parameter:"OrderBy,kind=query,in=sort,required=false" querySelector:"view=rows,property=orderBy"`
	ChildFields []string `parameter:"ChildFields,kind=query,in=childFields,required=false" querySelector:"view=children,property=fields"`
	ChildOrder  []string `parameter:"ChildOrder,kind=query,in=childSort,required=false" querySelector:"view=children,property=orderBy"`
}
type Component struct {
	Contract xdatly.Component[Input, Output] `component:"AliasRecords,path=/alias-records,method=GET" ignoreEmptyQueryParameters:"false"`
}

func (Component) DatlyQuerySelectors() selectors { return selectors{} }

var Linked = new(Component)
