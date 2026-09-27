package security

import xdatly "github.com/viant/xdatly"

// Linked, but not selected. Loading predicate authority must not activate this
// component or try to resolve its deliberately unavailable SQL resource.
type Unrelated struct {
	Read xdatly.Component[struct{}, UnrelatedOutput] `component:"Unrelated,path=/unrelated,method=GET,view=unrelated,connector=main"`
}

type UnrelatedOutput struct {
	Data []struct{ ID int } `parameter:",kind=output,in=view" view:"unrelated" sql:"uri=missing.sql"`
}
