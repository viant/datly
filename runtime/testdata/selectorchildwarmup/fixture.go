package selectorchildwarmup

import "github.com/viant/xdatly"

type Input struct {
	ID  int       `parameter:"ID,kind=query,in=id,required=true"`
	Has *InputHas `setMarker:"true" json:"-"`
}

type InputHas struct{ ID bool }

type Output struct {
	Data []*Row `parameter:"Data,kind=output,in=view" view:"rows,type=Row,limit=2" sql:"SELECT id,label FROM records WHERE id=:ID"`
}

type Row struct {
	ID       int      `sqlx:"id"`
	Label    string   `sqlx:"label"`
	Children []*Child `sqlx:"-"`
}

type Child struct {
	ID       int    `sqlx:"id"`
	ParentID int    `sqlx:"parent_id"`
	Label    string `sqlx:"label"`
}

type selectors struct {
	Fields []string `parameter:"Fields,kind=query,in=fields,required=false" querySelector:"view=rows,property=fields"`
	Page   int      `parameter:"Page,kind=query,in=page,required=false" querySelector:"view=rows,property=page"`
}

type Component struct {
	Contract xdatly.Component[Input, Output] `component:"ReadRecords,path=/selector-records,method=GET"`
}

func (Component) DatlyQuerySelectors() selectors { panic("metadata method must not be invoked") }

var Linked = new(Component)
