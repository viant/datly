package linkeddefault

import (
	"context"
	"reflect"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/xdatly"
	"github.com/viant/xdatly/handler"
)

type Component struct {
	Run xdatly.Component[Input, Output] `component:"Run,path=/linked-default,method=POST,handler=NewRun"`
}

var LinkedType = reflect.TypeFor[Component]()

func (Component) DatlyHandler(name string) func() (rhandler.TypedHandler, error) {
	if name == "NewRun" {
		return custom.Factory(NewRun)
	}
	return nil
}

type Input struct{}
type Output struct {
	Ready bool `json:"ready"`
}

type run struct{}

func NewRun() handler.Contract[Input, Output] { return &run{} }

func (*run) Exec(ctx context.Context, session handler.Session, _ *Input, output *Output) error {
	dependencies := struct {
		Starter handler.TransactionStarter `bind:"kind=transactionStarter,required"`
	}{}
	if err := session.Binder().Bind(ctx, &dependencies); err != nil {
		return err
	}
	output.Ready = dependencies.Starter != nil
	return nil
}
