package records

import (
	"embed"
	xdatly "github.com/viant/xdatly"
)

type Components struct {
	Read xdatly.Component[Input, Output] `component:"Records,path=/records,method=GET,connector=main,view=records,report=true" mcp:"[{\"kind\":\"tool\",\"name\":\"Records\"}]"`
}

type Input struct {
	Minimum int `parameter:"Minimum,kind=query,in=min,required" predicate:"handler,example.com/predicateapp/security.Minimum"`
}

//go:embed records.sql
var queries embed.FS

func (*Input) EmbedFS() *embed.FS { return &queries }

type Output struct {
	Data []*Row `parameter:",kind=output,in=view" view:"records,groupable=true,selectorProjection=true" sql:"uri=records.sql" json:"data"`
}

type Row struct {
	ID    int `sqlx:"id" json:"id" groupable:"true"`
	Total int `sqlx:"total" json:"total"`
}
