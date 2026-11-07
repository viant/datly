package companionchild

import "github.com/viant/xdatly"

type Input struct{}
type Output struct {
	Data []*Row `parameter:"Data,kind=output,in=view" view:"rows,type=Row,limit=25,selectorPage=true" sql:"SELECT r.* FROM (SELECT id FROM records) r"`
}
type Row struct {
	ID       int      `sqlx:"id"`
	Children []*Child `on:"ID:rows.id=ParentID:children.parent_id" view:"children,type=Child,selectorLimit=true,selectorOffset=true,selectorNoLimit=true" sql:"SELECT id,parent_id FROM children"`
}
type Child struct {
	ID       int `sqlx:"id"`
	ParentID int `sqlx:"parent_id"`
}
type selectors struct {
	Fields     []string `parameter:"Fields,kind=query,in=fields,required=false" querySelector:"view=rows,property=fields"`
	RootLimit  int      `parameter:"RootLimit,kind=query,in=root_limit,required=false" querySelector:"view=rows,property=limit"`
	RootOffset int      `parameter:"RootOffset,kind=query,in=root_offset,required=false" querySelector:"view=rows,property=offset"`
	Page       int      `parameter:"Page,kind=query,in=page,required=false" querySelector:"view=rows,property=page"`
	Limit      int      `parameter:"Limit,kind=query,in=limit,required=false" querySelector:"view=children,property=limit"`
	Offset     int      `parameter:"Offset,kind=query,in=offset,required=false" querySelector:"view=children,property=offset"`
}
type Component struct {
	Contract xdatly.Component[Input, Output] `component:"ChildSelectors,path=/child-selectors,method=GET" ignoreEmptyQueryParameters:"false"`
}

func (Component) DatlyQuerySelectors() selectors { return selectors{} }

var Linked = new(Component)
