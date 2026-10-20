package writer

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
	h "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

type outputlessAuxiliaryBody struct {
	Ids []int `sqlx:"ids,enc=JSON"`
}
type outputlessAuxiliaryInput struct {
	Data         *outputlessAuxiliaryBody   `parameter:"Data,kind=body,in=data" view:"Request,table=parents,auxiliary=true"`
	CurParents   []*outputlessAuxiliaryBody `parameter:"CurParents,kind=view,in=CurParents" view:"CurParents,table=parents"`
	OtherParents []*outputlessAuxiliaryBody `parameter:"OtherParents,kind=view,in=OtherParents" view:"OtherParents,table=parents"`
}
type outputlessAuxiliaryOutput struct {
	Messages []string
	Entity   int64
}
type outputlessAuxiliaryProbe struct {
	input                          *outputlessAuxiliaryInput
	output                         *outputlessAuxiliaryOutput
	init, validate, finalize, bind int
	fail                           string
	cancel                         context.CancelFunc
}
type outputlessAuxiliaryHook struct{ probe *outputlessAuxiliaryProbe }

var outputlessAuxiliaryLinked = reflect.TypeFor[outputlessAuxiliaryHook]()

func (hook *outputlessAuxiliaryHook) Init(_ context.Context, input *outputlessAuxiliaryInput, state h.LifecycleContext[outputlessAuxiliaryInput, h.NoParent, outputlessAuxiliaryOutput]) error {
	p := hook.probe
	p.init++
	if p.fail == "init" {
		return fmt.Errorf("init rejected")
	}
	if p.cancel != nil {
		p.cancel()
	}
	if input != p.input || state.Output != p.output {
		return fmt.Errorf("canonical input/output holder was replaced")
	}
	if state.Previous != nil || state.Parent != nil || state.Original != nil || state.PreviousFields != nil || state.SelfParent != nil {
		return fmt.Errorf("component lifecycle acquired row state")
	}
	state.Output.Messages = []string{"saved"}
	state.Output.Entity = 42
	return nil
}
func (hook *outputlessAuxiliaryHook) Validate(_ context.Context, input *outputlessAuxiliaryInput, state h.LifecycleContext[outputlessAuxiliaryInput, h.NoParent, outputlessAuxiliaryOutput]) error {
	hook.probe.validate++
	if hook.probe.fail == "validate" {
		return fmt.Errorf("validate rejected")
	}
	if input != hook.probe.input || state.Output != hook.probe.output || state.Output.Entity != 42 {
		return fmt.Errorf("validation replaced canonical state")
	}
	return nil
}
func (hook *outputlessAuxiliaryHook) Finalize(_ context.Context, input *outputlessAuxiliaryInput, output *outputlessAuxiliaryOutput, _ h.Outcome) error {
	hook.probe.finalize++
	if input != hook.probe.input || output != hook.probe.output {
		return fmt.Errorf("finalization replaced canonical state")
	}
	return nil
}

type outputlessAuxiliaryBadHook struct{}

var outputlessAuxiliaryBadLinked = reflect.TypeFor[outputlessAuxiliaryBadHook]()

func (*outputlessAuxiliaryBadHook) Init(context.Context, *outputlessAuxiliaryBody, h.LifecycleContext[outputlessAuxiliaryBody, h.NoParent, outputlessAuxiliaryOutput]) error {
	return nil
}
func (*outputlessAuxiliaryBadHook) Validate(context.Context, *outputlessAuxiliaryBody, h.LifecycleContext[outputlessAuxiliaryBody, h.NoParent, outputlessAuxiliaryOutput]) error {
	return nil
}

type outputlessAuxiliaryBinder struct {
	t     *testing.T
	probe *outputlessAuxiliaryProbe
}

func (binder *outputlessAuxiliaryBinder) Bind(_ context.Context, value any) error {
	hook, ok := value.(*outputlessAuxiliaryHook)
	if !ok {
		binder.t.Fatalf("unexpected binding %T", value)
	}
	binder.probe.bind++
	hook.probe = binder.probe
	return nil
}
func (binder *outputlessAuxiliaryBinder) Lookup(_ context.Context, key h.ValueKey) (any, bool, error) {
	binder.t.Fatalf("keyless auxiliary lifecycle requested row capability %s (DML/sequence/Current/validation must remain unused)", key)
	return nil, false, nil
}
func outputlessAuxiliaryComponent(hook string) *spec.Component {
	return &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: outputlessAuxiliaryLinked.PkgPath(), Name: "Request"}, Name: "Request", Settings: &spec.Settings{}, Routes: []*spec.Route{{Method: "PATCH", Path: "/keyless-auxiliary"}}, RootView: &spec.View{Name: "Request", Auxiliary: true, EntityHooks: hook, Source: &spec.ViewSource{Table: "parents"}, Columns: []*spec.Column{{Name: "ids", Source: "ids", Type: spec.TypeRef{Name: "[]int"}}}}}
}
func TestOutputlessAuxiliaryLeafLifecycleCanonicalInput(t *testing.T) {
	for _, mode := range []string{"nil", "empty", "populated", "same table Current"} {
		t.Run(mode, func(t *testing.T) {
			component := outputlessAuxiliaryComponent(outputlessAuxiliaryLinked.Name())
			native, err := New(component, reflect.TypeFor[outputlessAuxiliaryInput](), reflect.TypeFor[outputlessAuxiliaryOutput](), "patch")
			if err != nil {
				t.Fatalf("keyless auxiliary leaf with canonical lifecycle must compile: %v", err)
			}
			if native.metadata.OutputField != -1 {
				t.Fatal("missing output must retain sentinel")
			}
			input := &outputlessAuxiliaryInput{}
			if mode == "empty" {
				input.Data = &outputlessAuxiliaryBody{}
			} else if mode == "populated" || mode == "same table Current" {
				input.Data = &outputlessAuxiliaryBody{Ids: []int{7, 9}}
			}
			if mode == "same table Current" {
				input.CurParents = []*outputlessAuxiliaryBody{{Ids: []int{99}}}
				input.OtherParents = []*outputlessAuxiliaryBody{{Ids: []int{100}}}
				if native.metadata.CurrentField != -1 || native.metadata.CurrentType != nil {
					t.Fatal("component lifecycle must not select row Current")
				}
			}
			snapshot, err := native.CaptureInput(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			program := snapshot.(*Program)
			output := program.output.(*outputlessAuxiliaryOutput)
			probe := &outputlessAuxiliaryProbe{input: input, output: output}
			invocation := rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &outputlessAuxiliaryBinder{t: t, probe: probe}}
			result, err := native.Execute(context.Background(), invocation)
			if err != nil {
				t.Fatal(err)
			}
			if result != output || output.Entity != 42 || !reflect.DeepEqual(output.Messages, []string{"saved"}) {
				t.Fatalf("output/input holder changed: result=%+v input=%+v", result, input)
			}
			if mode == "same table Current" && (input.CurParents[0].Ids[0] != 99 || input.OtherParents[0].Ids[0] != 100) {
				t.Fatal("component lifecycle changed independent same-table reads")
			}
			if err := native.FinalizeOutcome(context.Background(), invocation, result, h.Outcome{}); err != nil {
				t.Fatal(err)
			}
			if probe.init != 1 || probe.validate != 1 || probe.finalize != 1 || probe.bind != 1 {
				t.Fatalf("canonical lifecycle counts=%+v", probe)
			}
			if len(program.frames.Rows) != 0 || len(program.actions.Rows) != 0 || native.RequiresPreBindingTransaction() {
				t.Fatalf("keyless component acquired row mutation machinery: frames=%d actions=%d prebinding=%v", len(program.frames.Rows), len(program.actions.Rows), native.RequiresPreBindingTransaction())
			}
		})
	}
}

func TestOutputlessAuxiliaryOperationsAndFailures(t *testing.T) {
	for _, operation := range []string{"post", "patch", "put"} {
		for _, failure := range []string{"", "init", "validate", "cancel"} {
			t.Run(operation+"/"+failure, func(t *testing.T) {
				component := outputlessAuxiliaryComponent(outputlessAuxiliaryLinked.Name())
				native, err := New(component, reflect.TypeFor[outputlessAuxiliaryInput](), reflect.TypeFor[outputlessAuxiliaryOutput](), operation)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				input := &outputlessAuxiliaryInput{}
				snapshot, err := native.CaptureInput(ctx, input)
				if err != nil {
					t.Fatal(err)
				}
				program := snapshot.(*Program)
				output := program.output.(*outputlessAuxiliaryOutput)
				probe := &outputlessAuxiliaryProbe{input: input, output: output, fail: failure}
				if failure == "cancel" {
					probe.cancel = cancel
				}
				invocation := rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &outputlessAuxiliaryBinder{t: t, probe: probe}}
				result, err := native.Execute(ctx, invocation)
				if (err != nil) != (failure != "") {
					t.Fatalf("failure=%s err=%v", failure, err)
				}
				if result != output {
					t.Fatalf("canonical output replaced: %T", result)
				}
				for i := 0; i < 2; i++ {
					if err := native.FinalizeOutcome(context.Background(), invocation, result, h.Outcome{}); err != nil {
						t.Fatal(err)
					}
				}
				if probe.finalize != 1 || probe.init != 1 || probe.bind != 1 {
					t.Fatalf("lifecycle counts %+v", probe)
				}
				expectedValidate := 1
				if failure == "init" || failure == "cancel" {
					expectedValidate = 0
				}
				if probe.validate != expectedValidate {
					t.Fatalf("validation ordering %+v", probe)
				}
				if len(program.frames.Rows) != 0 || len(program.actions.Rows) != 0 {
					t.Fatal("auxiliary lifecycle acquired physical work")
				}
			})
		}
	}
}
func TestOutputlessPhysicalAndMalformedWritersRejected(t *testing.T) {
	for _, mode := range []string{"physical post", "physical patch", "bad hook", "writable child"} {
		t.Run(mode, func(t *testing.T) {
			component := outputlessAuxiliaryComponent(outputlessAuxiliaryLinked.Name())
			operation := "post"
			switch mode {
			case "physical post":
				component.RootView.Auxiliary = false
			case "physical patch":
				component.RootView.Auxiliary = false
				operation = "patch"
			case "bad hook":
				component.RootView.EntityHooks = outputlessAuxiliaryBadLinked.Name()
			case "writable child":
				component.RootView.Relations = []*spec.Relation{{Name: "Children", Holder: "Children", View: &spec.View{Name: "Children", Source: &spec.ViewSource{Table: "children"}}}}
			}
			inputType := reflect.TypeFor[outputlessAuxiliaryInput]()
			if mode == "physical post" || mode == "physical patch" {
				inputType = reflect.TypeFor[keylessWritableInput]()
			}
			if _, err := New(component, inputType, reflect.TypeFor[outputlessAuxiliaryOutput](), operation); err == nil {
				t.Fatal("invalid outputless writer admitted")
			}
		})
	}
}

func TestOutputlessAuxiliaryCapturedErrorOutput(t *testing.T) {
	native, err := New(outputlessAuxiliaryComponent(outputlessAuxiliaryLinked.Name()), reflect.TypeFor[outputlessAuxiliaryInput](), reflect.TypeFor[outputlessAuxiliaryOutput](), "post")
	if err != nil {
		t.Fatal(err)
	}
	input := &outputlessAuxiliaryInput{}
	snapshot, err := native.CaptureInput(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	payload := &outputlessAuxiliaryOutput{Messages: []string{"business failure"}, Entity: 7}
	result, err := native.CapturedErrorOutput(t.Context(), rhandler.Invocation{Input: input, Snapshot: snapshot}, &response.Error{Code: 400, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	output := snapshot.(*Program).output.(*outputlessAuxiliaryOutput)
	if result != output || output == payload || output.Entity != 7 || !reflect.DeepEqual(output.Messages, payload.Messages) {
		t.Fatalf("canonical business output lost: %+v", result)
	}
}
