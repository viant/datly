package selectorcatalog

import "github.com/viant/xdatly"

type RootInput struct {
	Child *ChildOutput `parameter:"Child,kind=component,in=GET:/catalog-child"`
}
type ChildInput struct {
	Token string `parameter:"Token,kind=query,in=token,required=true"`
}
type RootOutput struct {
	Data []*Row `parameter:"Data,kind=output,in=view" view:"root,type=Row" sql:"SELECT id FROM records"`
}
type ChildOutput struct {
	Data []*Row `parameter:"Data,kind=output,in=view" view:"child,type=Row" sql:"SELECT id FROM records"`
}
type Row struct {
	ID int `sqlx:"id"`
}
type rootSelectors struct {
	Limit int `parameter:"Limit,kind=query,in=root_limit,required=false" querySelector:"view=root,property=limit"`
	Page  int `parameter:"Page,kind=query,in=root_page,required=false" querySelector:"view=root,property=page"`
}
type childSelectors struct {
	Limit  int      `parameter:"Limit,kind=query,in=child_limit,required=false" querySelector:"view=child,property=limit"`
	Fields []string `parameter:"Fields,kind=query,in=child_fields,required=false" querySelector:"view=child,property=fields"`
	Offset int      `parameter:"Offset,kind=query,in=child_offset,required=true" querySelector:"view=child,property=offset"`
}
type Root struct {
	Contract xdatly.Component[RootInput, RootOutput] `component:"Root,path=/catalog-root,method=GET"`
}
type Child struct {
	Contract xdatly.Component[ChildInput, ChildOutput] `component:"Child,path=/catalog-child,method=GET"`
}

func (Root) DatlyQuerySelectors() rootSelectors   { panic("metadata only") }
func (Child) DatlyQuerySelectors() childSelectors { panic("metadata only") }

var RootLinked = new(Root)
var ChildLinked = new(Child)

type conflictSelectors struct {
	Offset int `parameter:"Offset,kind=query,in=root_offset,required=false" querySelector:"view=root,property=offset"`
}
type Conflict struct {
	Contract xdatly.Component[RootInput, RootOutput] `component:"Conflict,path=/catalog-conflict,method=GET"`
}

func (Conflict) DatlyQuerySelectors() conflictSelectors { panic("metadata only") }

var ConflictLinked = new(Conflict)
