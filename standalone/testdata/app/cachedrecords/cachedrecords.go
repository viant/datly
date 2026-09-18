package cachedrecords

import (
	"reflect"

	"github.com/viant/x"
	"github.com/viant/xdatly"
)

type Component struct {
	Read xdatly.Component[Input, Output] `component:"Read,path=/cached-records,method=GET,connector=main,view=parents"`
}

var DatlyLinkedType = reflect.TypeFor[Component]()

type Input struct{}
type Output struct {
	Rows []*Parent `parameter:"Rows,kind=output,in=view" view:"parents,table=records" json:"rows"`
}
type Parent struct {
	ID       int      `sqlx:"id" json:"id"`
	Name     string   `sqlx:"name" json:"name"`
	Children []*Child `view:"children,table=cache_children,cache=shared" on:"ID:id=ParentID:parent_id" json:"children"`
}
type Child struct {
	ID       int       `sqlx:"id" json:"id"`
	ParentID int       `sqlx:"parent_id" json:"parentId"`
	Name     string    `sqlx:"name" json:"name"`
	Details  []*Detail `view:"details,table=cache_details,cache=shared" on:"ID:id=ChildID:child_id" json:"details"`
}
type Detail struct {
	ID      int    `sqlx:"id" json:"id"`
	ChildID int    `sqlx:"child_id" json:"childId"`
	Name    string `sqlx:"name" json:"name"`
}

func Exports() *x.Registry {
	r := x.NewRegistry()
	for _, typ := range []reflect.Type{reflect.TypeFor[Input](), reflect.TypeFor[Output](), reflect.TypeFor[Parent](), reflect.TypeFor[Child](), reflect.TypeFor[Detail]()} {
		r.Register(x.NewType(typ))
	}
	return r
}
