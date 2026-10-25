package executionoptions

import (
	"reflect"

	"github.com/viant/xdatly"
)

type Component struct {
	Read xdatly.Component[Input, Output] `component:"Read,path=/execution-hints/{id},method=GET,connector=main,view=records" mcp:"[{\"kind\":\"tool\",\"name\":\"HintedRead\"}]"`
}

type Input struct {
	ID int `parameter:"ID,kind=path,in=id,required" json:"id"`
}

type Record struct {
	ID   int    `sqlx:"id" json:"id"`
	Name string `sqlx:"name" json:"name"`
}

type Output struct {
	Rows []*Record `parameter:"Rows,kind=output,in=view" view:"records,cache=hinted,mysqlConditionFanoutFilter=false" sql:"SELECT id,name FROM records WHERE id=:ID" json:"rows"`
}

var LinkedType = reflect.TypeFor[Component]()
