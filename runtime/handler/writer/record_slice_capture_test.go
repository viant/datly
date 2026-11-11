package writer

import (
	"context"
	"github.com/viant/bindly"
	rhandler "github.com/viant/datly/runtime/handler"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type recordSliceCaptureInput struct {
	Data []keylessAuxiliaryBody `parameter:"Data,kind=body" view:"Request,table=parents,auxiliary=true"`
}
type recordSliceCaptureOutput struct {
	Data []keylessAuxiliaryBody `parameter:"Data,kind=output,in=body"`
}

func TestCaptureInputRecordSlice(t *testing.T) {
	component := keylessAuxiliaryComponent(recordSliceCaptureLinked.Name())
	native, err := New(component, reflect.TypeFor[recordSliceCaptureInput](), reflect.TypeFor[recordSliceCaptureOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	input := &recordSliceCaptureInput{Data: []keylessAuxiliaryBody{{Ids: []int{7, 9}}, {Ids: []int{11}}}}
	snapshot, err := native.CaptureInput(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	invocation := rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: recordSliceCaptureBinder{}}
	result, err := native.Execute(t.Context(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	if err := native.FinalizeOutcome(t.Context(), invocation, result, h.Outcome{}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.(*recordSliceCaptureOutput).Data, input.Data) {
		t.Fatal("execution lost slice output")
	}
	if !reflect.DeepEqual(input.Data[0].Ids, []int{7, 9}) || !reflect.DeepEqual(input.Data[1].Ids, []int{11}) {
		t.Fatal("capture changed input records")
	}
}

type recordSliceCaptureHook struct{}

var recordSliceCaptureLinked = reflect.TypeFor[recordSliceCaptureHook]()

func (*recordSliceCaptureHook) Init(context.Context, *recordSliceCaptureInput, h.LifecycleContext[recordSliceCaptureInput, h.NoParent, recordSliceCaptureOutput]) error {
	return nil
}
func (*recordSliceCaptureHook) Validate(context.Context, *recordSliceCaptureInput, h.LifecycleContext[recordSliceCaptureInput, h.NoParent, recordSliceCaptureOutput]) error {
	return nil
}

type recordSliceCaptureBinder struct{}

func (recordSliceCaptureBinder) Bind(context.Context, any, ...bindly.BindOption) error { return nil }
func (recordSliceCaptureBinder) Lookup(context.Context, h.ValueKey) (any, bool, error) {
	return nil, false, nil
}
