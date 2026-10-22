package independentdefault

import (
	"context"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/xdatly"
	"github.com/viant/xdatly/handler"
	"reflect"
)

type Component struct {
	Run xdatly.Component[Input, Output] `component:"Run,path=/independent-default,method=POST,handler=NewRun" independentChildTransactions:"true"`
}

var LinkedType = reflect.TypeFor[Component]()

func (Component) DatlyHandler(name string) func() (rh.TypedHandler, error) {
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
func (*run) Exec(_ context.Context, _ handler.Session, _ *Input, out *Output) error {
	out.Ready = true
	return nil
}
