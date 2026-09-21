package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

type preBindingData struct{ started bool }

func (d *preBindingData) Start(context.Context) error                       { d.started = true; return nil }
func (*preBindingData) Insert(string, any) error                            { return nil }
func (*preBindingData) Update(string, any) error                            { return nil }
func (*preBindingData) Delete(string, any) error                            { return nil }
func (*preBindingData) Execute(string, ...any) error                        { return nil }
func (*preBindingData) Allocate(context.Context, string, any, string) error { return nil }
func (*preBindingData) Flush(context.Context, string) error                 { return nil }

var _ xhandler.Data = (*preBindingData)(nil)
var _ xhandler.TransactionStarter = (*preBindingData)(nil)

type preBindingSource struct{ data *preBindingData }

func (s preBindingSource) Open(context.Context) (xhandler.Data, error) { return s.data, nil }

var _ dexec.DataSource = preBindingSource{}

type preBindingInput struct{ data *preBindingData }

func (i *preBindingInput) Init(context.Context) error {
	if i.data == nil || !i.data.started {
		return errors.New("transaction was not started before input initialization")
	}
	return nil
}

type preBindingHandler struct{}

func (*preBindingHandler) RequiresPreBindingTransaction() bool { return true }
func (*preBindingHandler) Execute(context.Context, rhandler.Invocation) (any, error) {
	return "ok", nil
}

func TestPreBindingTransactionStartsBeforeInputInitialization(t *testing.T) {
	data := &preBindingData{}
	input := &preBindingInput{data: data}
	actual, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeFor[preBindingInput]()), BoundInput: input, DataSource: preBindingSource{data: data}, Handler: &preBindingHandler{}})
	if err != nil || actual != "ok" {
		t.Fatalf("execute result=%v err=%v", actual, err)
	}
	if !data.started {
		t.Fatal("transaction was not started")
	}
}
