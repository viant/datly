package documented

import (
	"embed"
	xdatly "github.com/viant/xdatly"
)

//go:embed *.yaml
var Files embed.FS

type Input struct {
	Search   string `parameter:"Search,kind=query,in=search" json:"query"`
	Explicit string `parameter:"Explicit,kind=query,in=explicit" desc:"Authored parameter" example:"authored-example"`
}
type Row struct {
	ID   int    `sqlx:"id" json:"identifier"`
	Name string `sqlx:"name" json:"name" desc:"Authored name" example:"Ada"`
}
type Output struct {
	Rows []Row `parameter:"Rows,kind=output,in=view" view:"users,table=users" json:"rows"`
}
type Holder struct {
	Route xdatly.Component[Input, Output] `component:"Records,path=/records,method=GET" docURL:"pkg:rule.yaml" mcp:"[{\"kind\":\"tool\",\"name\":\"records\"}]"`
}
