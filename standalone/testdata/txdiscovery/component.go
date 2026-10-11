package txdiscovery

import (
	"context"
	"errors"
	"github.com/viant/datly/exec"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/xdatly"
	"github.com/viant/xdatly/handler"
	"reflect"
)

type Component struct {
	Setup xdatly.Component[ReadInput, Output]     `component:"Setup,path=/tx-discovery-setup,method=POST,connector=main,handler=NewSetup"`
	Late  xdatly.Component[ReadInput, LateOutput] `component:"Late,path=/tx-discovery-late,method=GET,connector=main"`
	Run   xdatly.Component[Input, Output]         `component:"Run,path=/tx-discovery,method=POST,connector=main,handler=NewRun"`
	Read  xdatly.Component[ReadInput, ReadOutput] `component:"Read,path=/tx-discovery-read,method=GET,connector=main"`
}

var LinkedType = reflect.TypeFor[Component]()

type Input struct {
	Fail bool `parameter:"Fail,kind=body,in=fail"`
}
type Output struct {
	Count int `json:"count"`
}
type ReadInput struct{}
type Row struct {
	ID int `sqlx:"id" json:"id"`
}
type ReadOutput struct {
	Rows []*Row `json:"rows" parameter:"Rows,kind=output,in=view" view:"Rows" sql:"SELECT 1 AS id"`
}

func (Component) DatlyHandler(name string) func() (rh.TypedHandler, error) {
	if name == "NewSetup" {
		return custom.Factory(NewSetup)
	}
	if name == "NewRun" {
		return custom.Factory(NewRun)
	}
	return nil
}

type run struct{}

func NewRun() handler.Contract[Input, Output] { return &run{} }
func (*run) Exec(ctx context.Context, s handler.Session, in *Input, out *Output) error {
	deps := struct {
		Start   handler.TransactionStarter `bind:"kind=transactionStarter,required"`
		Invoker exec.ComponentInvoker      `bind:"kind=component_invoker,required"`
	}{}
	if e := s.Binder().Bind(ctx, &deps); e != nil {
		return e
	}
	if e := deps.Start.Start(ctx); e != nil {
		return e
	}
	raw, e := deps.Invoker.InvokeComponent(ctx, exec.ComponentRequest{Target: exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: LinkedType.PkgPath(), Name: "Read"}, Route: spec.RouteRef{Method: "GET", Path: "/tx-discovery-read"}}, Input: &ReadInput{}})
	if e != nil {
		return e
	}
	result, ok := raw.(*ReadOutput)
	if !ok || len(result.Rows) != 1 || result.Rows[0].ID != 1 {
		return errors.New("owned reader unavailable")
	}
	out.Count = 1
	if in.Fail {
		return errors.New("fixture outer rejection")
	}
	return nil
}

type LateOutput struct {
	Rows []*Row `json:"rows" parameter:"Rows,kind=output,in=view" view:"Rows" sql:"SELECT id FROM late_fixture"`
}
type setup struct{}

func NewSetup() handler.Contract[ReadInput, Output] { return &setup{} }
func (*setup) Exec(ctx context.Context, s handler.Session, _ *ReadInput, out *Output) error {
	deps := struct {
		DML handler.DML `bind:"kind=dml,required"`
	}{}
	if e := s.Binder().Bind(ctx, &deps); e != nil {
		return e
	}
	if e := deps.DML.Execute("CREATE TABLE late_fixture(id INTEGER NOT NULL)"); e != nil {
		return e
	}
	if e := deps.DML.Execute("INSERT INTO late_fixture(id) VALUES(7)"); e != nil {
		return e
	}
	out.Count = 1
	return nil
}
