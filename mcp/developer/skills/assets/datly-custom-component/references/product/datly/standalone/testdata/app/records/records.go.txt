package records

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/x"
	xdatly "github.com/viant/xdatly"
	h "github.com/viant/xdatly/handler"
)

type Component struct {
	Read  xdatly.Component[ReadInput, ReadOutput]   `component:"Read,path=/records/{id},method=GET,connector=main,view=records"`
	Write xdatly.Component[WriteInput, WriteOutput] `component:"Write,path=/records,method=POST,connector=main,handler=NewWrite"`
}

type Record struct {
	ID   int    `sqlx:"id,primaryKey=true" json:"id"`
	Name string `sqlx:"name" json:"name"`
}
type ReadInput struct {
	ID int `parameter:"ID,kind=path,in=id,required"`
}
type ReadOutput struct {
	Rows []*Record `parameter:"Rows,kind=output,in=view" view:"records,table=records" sql:"uri=queries/read.sql" json:"rows"`
}
type WriteInput struct {
	Record      *Record `parameter:"Record,kind=body,in=data,required"`
	Initialized bool    `json:"-"`
}

func (i *WriteInput) Init(context.Context) error { i.Initialized = true; return nil }

type WriteOutput struct {
	Data      *Record `json:"data"`
	Finalized bool    `json:"finalized"`
}

func (o *WriteOutput) Finalize(context.Context) error { o.Finalized = true; return nil }

type writer struct{}

func NewWrite() h.Contract[WriteInput, WriteOutput] { return &writer{} }
func (*writer) Exec(ctx context.Context, session h.Session, input *WriteInput, output *WriteOutput) error {
	if !input.Initialized {
		return fmt.Errorf("input lifecycle did not run")
	}
	value, found, err := session.Binder().Lookup(ctx, h.DMLKey)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("DML unavailable")
	}
	if err := value.(h.DML).Insert("records", input.Record); err != nil {
		return err
	}
	output.Data = input.Record
	return nil
}

// Exports is the same typed linking surface an authored application supplies.
func Exports() (*x.Registry, error) {
	r := x.NewRegistry()
	for _, typ := range []reflect.Type{reflect.TypeFor[Record](), reflect.TypeFor[ReadInput](), reflect.TypeFor[ReadOutput](), reflect.TypeFor[WriteInput](), reflect.TypeFor[WriteOutput]()} {
		r.Register(x.NewType(typ))
	}
	f, err := x.NewFunction(reflect.TypeFor[Component]().PkgPath(), "NewWrite", custom.Factory(NewWrite))
	if err != nil {
		return nil, err
	}
	return r, r.RegisterFunctions(f)
}
