package selectorpolicy

import (
	"reflect"

	"github.com/viant/xdatly"
)

type Components struct {
	Deny  xdatly.Component[Input, DeniedOutput]   `component:"Denied,path=/selector-denied,method=GET,connector=main" mcp:"[{\"kind\":\"tool\",\"name\":\"Denied\"}]"`
	Allow xdatly.Component[Input, AllowedOutput]  `component:"Allowed,path=/selector-allowed,method=GET,connector=main" mcp:"[{\"kind\":\"tool\",\"name\":\"Allowed\"}]"`
	Infer xdatly.Component[Input, InferredOutput] `component:"Inferred,path=/selector-inferred,method=GET,connector=main" mcp:"[{\"kind\":\"tool\",\"name\":\"Inferred\"}]"`
}

var Datly = new(Components)
var DatlyLinkedType = reflect.TypeFor[Components]()

type Input struct {
	OrderBy string `parameter:"OrderBy,kind=query,in=orderBy" querySelector:"view=rows,property=orderBy" json:"orderBy"`
	Page    int    `parameter:"Page,kind=query,in=page" querySelector:"view=rows,property=page" json:"page"`
}

type Row struct {
	ID int `sqlx:"id" json:"id"`
}

type DeniedOutput struct {
	Data []*Row `parameter:",kind=output,in=view" view:"rows,limit=1,selectorOrderBy=false,selectorPage=false" sql:"SELECT id FROM records" json:"data"`
}

type AllowedOutput struct {
	Data []*Row `parameter:",kind=output,in=view" view:"rows,limit=1,selectorOrderBy=true,selectorPage=true" sql:"SELECT id FROM records" json:"data"`
}

type InferredOutput struct {
	Data []*Row `parameter:",kind=output,in=view" view:"rows,limit=1" sql:"SELECT id FROM records" json:"data"`
}
