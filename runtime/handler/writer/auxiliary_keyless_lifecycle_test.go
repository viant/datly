package writer

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
	h "github.com/viant/xdatly/handler"
)

type keylessAuxiliaryBody struct {
	Ids []int `sqlx:"ids,enc=JSON"`
}
type keylessAuxiliaryInput struct {
	Data       *keylessAuxiliaryBody   `parameter:"Data,kind=body,in=data" view:"Request,table=parents,auxiliary=true"`
	CurParents []*keylessAuxiliaryBody `parameter:"CurParents,kind=view,in=CurParents" view:"CurParents,table=parents"`
}
type keylessAuxiliaryOutput struct {
	Writes *keylessAuxiliaryBody `parameter:"Writes,kind=output,in=body" json:"-"`
	Result []int
}
type keylessAuxiliaryProbe struct {
	input                          *keylessAuxiliaryInput
	output                         *keylessAuxiliaryOutput
	init, validate, finalize, bind int
}
type keylessAuxiliaryHook struct{ probe *keylessAuxiliaryProbe }

var keylessAuxiliaryLinked = reflect.TypeFor[keylessAuxiliaryHook]()

func (hook *keylessAuxiliaryHook) Init(_ context.Context, input *keylessAuxiliaryInput, state h.LifecycleContext[keylessAuxiliaryInput, h.NoParent, keylessAuxiliaryOutput]) error {
	p := hook.probe
	p.init++
	if input != p.input || state.Output != p.output || state.Output.Writes != input.Data {
		return fmt.Errorf("canonical input/output holder was replaced")
	}
	if state.Previous != nil || state.Parent != nil || state.Original != nil || state.PreviousFields != nil || state.SelfParent != nil {
		return fmt.Errorf("component lifecycle acquired row state")
	}
	state.Output.Result = []int{42}
	return nil
}
func (hook *keylessAuxiliaryHook) Validate(_ context.Context, input *keylessAuxiliaryInput, state h.LifecycleContext[keylessAuxiliaryInput, h.NoParent, keylessAuxiliaryOutput]) error {
	hook.probe.validate++
	if input != hook.probe.input || state.Output != hook.probe.output || state.Output.Writes != input.Data || !reflect.DeepEqual(state.Output.Result, []int{42}) {
		return fmt.Errorf("validation replaced canonical state")
	}
	return nil
}
func (hook *keylessAuxiliaryHook) Finalize(_ context.Context, input *keylessAuxiliaryInput, output *keylessAuxiliaryOutput, _ h.Outcome) error {
	hook.probe.finalize++
	if input != hook.probe.input || output != hook.probe.output {
		return fmt.Errorf("finalization replaced canonical state")
	}
	return nil
}

type keylessAuxiliaryBadHook struct{}

var keylessAuxiliaryBadLinked = reflect.TypeFor[keylessAuxiliaryBadHook]()

func (*keylessAuxiliaryBadHook) Init(context.Context, *keylessAuxiliaryBody, h.LifecycleContext[keylessAuxiliaryBody, h.NoParent, keylessAuxiliaryOutput]) error {
	return nil
}
func (*keylessAuxiliaryBadHook) Validate(context.Context, *keylessAuxiliaryBody, h.LifecycleContext[keylessAuxiliaryBody, h.NoParent, keylessAuxiliaryOutput]) error {
	return nil
}

type keylessAuxiliaryBinder struct {
	t     *testing.T
	probe *keylessAuxiliaryProbe
}

func (binder *keylessAuxiliaryBinder) Bind(_ context.Context, value any) error {
	hook, ok := value.(*keylessAuxiliaryHook)
	if !ok {
		binder.t.Fatalf("unexpected binding %T", value)
	}
	binder.probe.bind++
	hook.probe = binder.probe
	return nil
}
func (binder *keylessAuxiliaryBinder) Lookup(_ context.Context, key h.ValueKey) (any, bool, error) {
	binder.t.Fatalf("keyless auxiliary lifecycle requested row capability %s (DML/sequence/Current/validation must remain unused)", key)
	return nil, false, nil
}
func keylessAuxiliaryComponent(hook string) *spec.Component {
	return &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: keylessAuxiliaryLinked.PkgPath(), Name: "Request"}, Name: "Request", Settings: &spec.Settings{}, Routes: []*spec.Route{{Method: "PATCH", Path: "/keyless-auxiliary"}}, RootView: &spec.View{Name: "Request", Auxiliary: true, EntityHooks: hook, Source: &spec.ViewSource{Table: "parents"}, Columns: []*spec.Column{{Name: "ids", Source: "ids", Type: spec.TypeRef{Name: "[]int"}}}}}
}
func TestKeylessAuxiliaryLeafLifecycleCanonicalInput(t *testing.T) {
	for _, mode := range []string{"nil", "empty", "populated", "same table Current"} {
		t.Run(mode, func(t *testing.T) {
			component := keylessAuxiliaryComponent(keylessAuxiliaryLinked.Name())
			native, err := New(component, reflect.TypeFor[keylessAuxiliaryInput](), reflect.TypeFor[keylessAuxiliaryOutput](), "patch")
			if err != nil {
				t.Fatalf("keyless auxiliary leaf with canonical lifecycle must compile: %v", err)
			}
			input := &keylessAuxiliaryInput{}
			if mode == "empty" {
				input.Data = &keylessAuxiliaryBody{}
			} else if mode == "populated" || mode == "same table Current" {
				input.Data = &keylessAuxiliaryBody{Ids: []int{7, 9}}
			}
			if mode == "same table Current" {
				input.CurParents = []*keylessAuxiliaryBody{{Ids: []int{99}}}
				if native.metadata.CurrentField < 0 {
					t.Fatal("fixture must select same-table Current before bypassing row work")
				}
			}
			snapshot, err := native.CaptureInput(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			program := snapshot.(*Program)
			output := program.output.(*keylessAuxiliaryOutput)
			probe := &keylessAuxiliaryProbe{input: input, output: output}
			invocation := rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &keylessAuxiliaryBinder{t: t, probe: probe}}
			result, err := native.Execute(context.Background(), invocation)
			if err != nil {
				t.Fatal(err)
			}
			if result != output || output.Writes != input.Data || !reflect.DeepEqual(output.Result, []int{42}) {
				t.Fatalf("output/input holder changed: result=%+v input=%+v", result, input)
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
func TestKeylessAuxiliaryLifecycleRejectsUnsupportedShapes(t *testing.T) {
	for _, mode := range []string{"no hook", "unresolved hook", "wrong canonical hook", "writable", "relation bearing"} {
		t.Run(mode, func(t *testing.T) {
			component := keylessAuxiliaryComponent(keylessAuxiliaryLinked.Name())
			operation := "patch"
			switch mode {
			case "no hook":
				component.RootView.EntityHooks = ""
			case "unresolved hook":
				component.RootView.EntityHooks = "MissingKeylessAuxiliaryHook"
			case "wrong canonical hook":
				component.RootView.EntityHooks = keylessAuxiliaryBadLinked.Name()
			case "writable":
				component.RootView.Auxiliary = false
				operation = "post"
			case "relation bearing":
				component.RootView.Relations = []*spec.Relation{{Name: "Children", Holder: "Children", View: &spec.View{Name: "Children", Source: &spec.ViewSource{Table: "children"}}}}
				operation = "post"
			}
			inputType := reflect.TypeFor[keylessAuxiliaryInput]()
			if mode == "writable" {
				inputType = reflect.TypeFor[keylessWritableInput]()
			}
			if _, err := New(component, inputType, reflect.TypeFor[keylessAuxiliaryOutput](), operation); err == nil {
				t.Fatalf("unsupported keyless %s root compiled", mode)
			}
		})
	}
}

type keylessWritableInput struct {
	Data *keylessAuxiliaryBody `parameter:"Data,kind=body,in=data" view:"Request,table=parents"`
}

// The reflected holder alone must not turn the leaf exception into permission
// for a keyless graph, even when the canonical spec omitted the relation.
type keylessAuxiliaryGraphBody struct {
	Ids      []int                         `sqlx:"ids,enc=JSON"`
	Children []*keylessAuxiliaryGraphChild `view:"Children,table=children" on:"Ids=ParentIds"`
}
type keylessAuxiliaryGraphChild struct {
	ID        int   `sqlx:"id,primaryKey"`
	ParentIds []int `sqlx:"parent_ids,enc=JSON"`
}
type keylessAuxiliaryGraphInput struct {
	Data *keylessAuxiliaryGraphBody `parameter:"Data,kind=body,in=data" view:"Request,table=parents,auxiliary=true"`
}
type keylessAuxiliaryGraphOutput struct {
	Writes *keylessAuxiliaryGraphBody `parameter:"Writes,kind=output,in=body"`
}

func TestKeylessAuxiliaryLifecycleRejectsReflectedRelation(t *testing.T) {
	component := keylessAuxiliaryComponent(keylessAuxiliaryLinked.Name())
	if len(component.RootView.Relations) != 0 {
		t.Fatal("fixture must have no spec relation")
	}
	_, err := New(component, reflect.TypeFor[keylessAuxiliaryGraphInput](), reflect.TypeFor[keylessAuxiliaryGraphOutput](), "post")
	if err == nil || !strings.Contains(err.Error(), "no primary key") {
		t.Fatalf("reflected relation must retain root identity guard: %v", err)
	}
}
