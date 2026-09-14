package devapp

import (
	"context"
	"fmt"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/x"
	h "github.com/viant/xdatly/handler"
	"reflect"
)

type Record struct {
	ID   int    `sqlx:"id,primaryKey=true" json:"id"`
	Name string `sqlx:"name" json:"name"`
}
type ReadInput struct {
	ID int `parameter:"ID,kind=path,in=id,required"`
}
type ReadOutput struct {
	Rows []*Record `parameter:"Rows,kind=output,in=view" view:"Read,table=records" sql:"SELECT id,name FROM records WHERE id=:ID" json:"rows"`
}
type WriteInput struct {
	Record *Record `parameter:"Record,kind=body,in=data,required"`
}
type WriteOutput struct {
	Data *Record `json:"data"`
}
type writer struct{}

func NewWrite() h.Contract[WriteInput, WriteOutput] { return &writer{} }
func (*writer) Exec(ctx context.Context, session h.Session, input *WriteInput, output *WriteOutput) error {
	value, ok, err := session.Binder().Lookup(ctx, h.DMLKey)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("DML not available")
	}
	if err = value.(h.DML).Insert("records", input.Record); err != nil {
		return err
	}
	output.Data = input.Record
	return nil
}
func Exports() (*x.Registry, error) {
	r := x.NewRegistry()
	for _, t := range []reflect.Type{reflect.TypeFor[Record](), reflect.TypeFor[ReadInput](), reflect.TypeFor[ReadOutput](), reflect.TypeFor[WriteInput](), reflect.TypeFor[WriteOutput]()} {
		r.Register(x.NewType(t))
	}
	function, err := x.NewFunction(reflect.TypeFor[Record]().PkgPath(), "NewWrite", custom.Factory(NewWrite))
	if err != nil {
		return nil, err
	}
	return r, r.RegisterFunctions(function)
}
