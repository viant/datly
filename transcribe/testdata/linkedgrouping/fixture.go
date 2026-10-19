package linkedgrouping

import (
	"embed"
	"github.com/viant/xdatly"
)

//go:embed sql/*.sql
var Resources embed.FS

type Input struct{}
type Output struct {
	Data []*Row `parameter:"Data,kind=output,in=view" view:"Spend,type=Row,groupable=true" sql:"uri=sql/spend.sql"`
}
type Row struct {
	Region string `sqlx:"region"`
	Total  int    `sqlx:"total"`
}
type Component struct {
	Contract xdatly.Component[Input, Output] `component:"Spend,path=/linked-spend,method=GET,view=Spend"`
}

var Linked = new(Component)
