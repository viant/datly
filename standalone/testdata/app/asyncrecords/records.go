package asyncrecords

import (
	"context"
	"fmt"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/x"
	xdatly "github.com/viant/xdatly"
	xasync "github.com/viant/xdatly/async"
	h "github.com/viant/xdatly/handler"
	"reflect"
)

type Component struct {
	Write  xdatly.Component[Input, Output]         `component:"Write,path=/async-records,method=POST,connector=main,handler=NewWrite"`
	Status xdatly.Component[StatusInput, Output]   `component:"Status,path=/async-status/{jobid},method=GET,connector=main,handler=NewStatus"`
	Read   xdatly.Component[ReadInput, ReadOutput] `component:"Read,path=/async-read/{id},method=GET,connector=main,view=records"`
}
type Record struct {
	ID   int    `sqlx:"id,primaryKey=true" json:"id"`
	Name string `sqlx:"name" json:"name"`
}
type Input struct {
	Record      *Record     `parameter:"Record,kind=body,in=data,required"`
	Key         string      `parameter:"Key,kind=query,in=key,required"`
	Sync        bool        `parameter:"Sync,kind=query,in=wait"`
	JWT         *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required,errorCode=401" codec:"JwtClaim" json:"-"`
	Initialized bool        `json:"-"`
}

func (i *Input) Init(context.Context) error { i.Initialized = true; return nil }

type StatusInput struct {
	JobID string      `parameter:"JobID,kind=path,in=jobid,required"`
	JWT   *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required,errorCode=401" codec:"JwtClaim" json:"-"`
}
type ReadInput struct {
	ID   int         `parameter:"ID,kind=path,in=id,required"`
	Key  string      `parameter:"Key,kind=query,in=key,required"`
	Sync bool        `parameter:"Sync,kind=query,in=wait"`
	JWT  *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required,errorCode=401" codec:"JwtClaim" json:"-"`
}
type ReadOutput struct {
	Rows []*Record   `parameter:"Rows,kind=output,in=view" view:"records,table=records" sql:"SELECT id,name FROM records WHERE id=:ID" json:"rows"`
	Job  *xasync.Job `parameter:"kind=async,in=job" json:"job,omitempty"`
}
type Output struct {
	Job  *xasync.Job `parameter:"kind=async,in=job" json:"job,omitempty"`
	Code string      `parameter:"kind=async,in=jobinfo.code" json:"code"`
	Data *Record     `json:"data,omitempty"`
}
type writer struct{ gate func(context.Context) error }

func NewWrite() h.Contract[Input, Output] { return &writer{} }
func (w *writer) Exec(ctx context.Context, session h.Session, input *Input, output *Output) error {
	if !input.Initialized || input.JWT == nil || input.JWT.Subject != "approved" {
		return fmt.Errorf("missing initialization or verified JWT")
	}
	if w.gate != nil {
		if err := w.gate(ctx); err != nil {
			return err
		}
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

type status struct{}

func NewStatus() h.Contract[StatusInput, Output] { return &status{} }
func (*status) Exec(context.Context, h.Session, *StatusInput, *Output) error {
	return fmt.Errorf("status handler must not execute")
}

func Exports(gate func(context.Context) error) (*x.Registry, error) {
	r := x.NewRegistry()
	for _, typ := range []reflect.Type{reflect.TypeFor[Record](), reflect.TypeFor[Input](), reflect.TypeFor[StatusInput](), reflect.TypeFor[Output](), reflect.TypeFor[ReadInput](), reflect.TypeFor[ReadOutput](), reflect.TypeFor[jwt.Claims](), reflect.TypeFor[xasync.Job]()} {
		r.Register(x.NewType(typ))
	}
	w, err := x.NewFunction(reflect.TypeFor[Component]().PkgPath(), "NewWrite", custom.Factory(func() h.Contract[Input, Output] { return &writer{gate: gate} }))
	if err != nil {
		return nil, err
	}
	s, err := x.NewFunction(reflect.TypeFor[Component]().PkgPath(), "NewStatus", custom.Factory(NewStatus))
	if err != nil {
		return nil, err
	}
	return r, r.RegisterFunctions(w, s)
}
