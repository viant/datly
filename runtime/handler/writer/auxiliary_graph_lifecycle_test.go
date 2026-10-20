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

type auxiliaryGraphRow struct {
	Id       int                    `sqlx:"id,primaryKey"`
	Children []*auxiliaryGraphChild `view:"Children,table=children,auxiliary=true" on:"Id=ParentId"`
}
type auxiliaryGraphChild struct {
	Id       int `sqlx:"id,primaryKey"`
	ParentId int `sqlx:"parent_id"`
}
type auxiliaryGraphInput struct {
	Data        []*auxiliaryGraphRow   `parameter:"Data,kind=body,in=data" view:"Request,table=parents,auxiliary=true"`
	First       []*auxiliaryGraphRow   `parameter:"First,kind=view" view:"First,table=parents"`
	Second      []*auxiliaryGraphRow   `parameter:"Second,kind=view" view:"Second,table=parents"`
	FirstChild  []*auxiliaryGraphChild `parameter:"FirstChild,kind=view" view:"FirstChild,table=children"`
	SecondChild []*auxiliaryGraphChild `parameter:"SecondChild,kind=view" view:"SecondChild,table=children"`
}
type auxiliaryGraphOutput struct {
	Data []*auxiliaryGraphRow `parameter:"Data,kind=output,in=body"`
}
type auxiliaryGraphProbe struct {
	bind, init, validate, finalize int
	fail                           string
}
type auxiliaryGraphHook struct{ probe *auxiliaryGraphProbe }

var auxiliaryGraphHookType = reflect.TypeFor[auxiliaryGraphHook]()

func (hook *auxiliaryGraphHook) Init(_ context.Context, input *auxiliaryGraphInput, state h.LifecycleContext[auxiliaryGraphInput, h.NoParent, auxiliaryGraphOutput]) error {
	hook.probe.init++
	if state.Previous != nil || state.Parent != nil || state.Original != nil || state.PreviousFields != nil || state.Output == nil {
		return fmt.Errorf("row state leaked")
	}
	if hook.probe.fail == "Init" {
		return fmt.Errorf("Init failed")
	}
	return nil
}
func (hook *auxiliaryGraphHook) Validate(context.Context, *auxiliaryGraphInput, h.LifecycleContext[auxiliaryGraphInput, h.NoParent, auxiliaryGraphOutput]) error {
	hook.probe.validate++
	if hook.probe.fail == "Validate" {
		return fmt.Errorf("Validate failed")
	}
	return nil
}
func (hook *auxiliaryGraphHook) Finalize(context.Context, *auxiliaryGraphInput, *auxiliaryGraphOutput, h.Outcome) error {
	hook.probe.finalize++
	return nil
}

type auxiliaryGraphBadPhase struct{ auxiliaryGraphHook }

var auxiliaryGraphBadPhaseType = reflect.TypeFor[auxiliaryGraphBadPhase]()

func (*auxiliaryGraphBadPhase) AfterValidateInput(context.Context, *auxiliaryGraphInput, *auxiliaryGraphOutput) error {
	return nil
}

type auxiliaryGraphBadQueuePhase struct{ auxiliaryGraphHook }

var auxiliaryGraphBadQueuePhaseType = reflect.TypeFor[auxiliaryGraphBadQueuePhase]()

func (*auxiliaryGraphBadQueuePhase) AfterQueueInput(context.Context, *auxiliaryGraphInput, *auxiliaryGraphOutput) error {
	return nil
}

type auxiliaryGraphBinder struct{ probe *auxiliaryGraphProbe }

func (b *auxiliaryGraphBinder) Bind(_ context.Context, value any) error {
	hook := value.(*auxiliaryGraphHook)
	hook.probe = b.probe
	b.probe.bind++
	return nil
}
func (*auxiliaryGraphBinder) Lookup(context.Context, h.ValueKey) (any, bool, error) {
	return nil, false, fmt.Errorf("auxiliary graph requested row capability")
}
func auxiliaryGraphComponent(hook string) *spec.Component {
	// Bootstrap's normal minimal source metadata is completed by reflected tags.
	return &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: auxiliaryGraphHookType.PkgPath(), Name: "Request"}, Settings: &spec.Settings{}, RootView: &spec.View{Name: "Request", EntityHooks: hook}}
}
func TestAuxiliaryGraphCanonicalLifecycle(t *testing.T) {
	for _, fail := range []string{"", "Init", "Validate"} {
		t.Run(fail, func(t *testing.T) {
			native, err := New(auxiliaryGraphComponent(auxiliaryGraphHookType.Name()), reflect.TypeFor[auxiliaryGraphInput](), reflect.TypeFor[auxiliaryGraphOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			if !native.metadata.componentLifecycle || native.metadata.CurrentField != -1 || native.metadata.Root.Relations[0].Child.CurrentField != -1 {
				t.Fatal("graph selected row Current")
			}
			input := &auxiliaryGraphInput{Data: []*auxiliaryGraphRow{{Id: 1, Children: []*auxiliaryGraphChild{{Id: 2, ParentId: 1}}}}}
			snapshot, err := native.CaptureInput(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			p := snapshot.(*Program)
			probe := &auxiliaryGraphProbe{fail: fail}
			invocation := rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &auxiliaryGraphBinder{probe: probe}}
			result, err := native.Execute(t.Context(), invocation)
			if (err != nil) != (fail != "") {
				t.Fatalf("execution error=%v", err)
			}
			wantValidate := 1
			if fail == "Init" {
				wantValidate = 0
			}
			if probe.bind != 1 || probe.init != 1 || probe.validate != wantValidate {
				t.Fatalf("calls=%+v", probe)
			}
			if len(p.frames.Rows) != 0 || len(p.actions.Rows) != 0 || native.RequiresPreBindingTransaction() {
				t.Fatal("auxiliary graph acquired ordinary mutation actions")
			}
			if fail == "" {
				for i := 0; i < 2; i++ {
					if err := native.FinalizeOutcome(t.Context(), invocation, result, h.Outcome{}); err != nil {
						t.Fatal(err)
					}
				}
				if probe.finalize != 1 {
					t.Fatalf("Finalize called %d times", probe.finalize)
				}
			}
		})
	}
}
func TestAuxiliaryGraphLifecycleAdmission(t *testing.T) {
	for _, mode := range []string{"wrong Input", "AfterValidateInput", "AfterQueueInput", "spec writable", "reflected writable"} {
		t.Run(mode, func(t *testing.T) {
			component := auxiliaryGraphComponent(auxiliaryGraphHookType.Name())
			input := reflect.TypeFor[auxiliaryGraphInput]()
			switch mode {
			case "wrong Input":
				component.RootView.EntityHooks = keylessAuxiliaryBadLinked.Name()
			case "AfterValidateInput":
				component.RootView.EntityHooks = auxiliaryGraphBadPhaseType.Name()
			case "AfterQueueInput":
				component.RootView.EntityHooks = auxiliaryGraphBadQueuePhaseType.Name()
			case "spec writable":
				component.RootView.Auxiliary = true
				component.RootView.Relations = []*spec.Relation{{Holder: "Children", View: &spec.View{Name: "Children", Source: &spec.ViewSource{Table: "children"}}}}
				input = reflect.TypeFor[keylessAuxiliaryGraphInput]()
			case "reflected writable":
				input = reflect.TypeFor[keylessAuxiliaryGraphInput]()
			}
			if strings.HasSuffix(mode, "writable") && auxiliaryComponentGraph(component.RootView, dereference(input.Field(0).Type), true) {
				t.Fatal("writable body classified as component lifecycle")
			}
			_, err := New(component, input, reflect.TypeFor[auxiliaryGraphOutput](), "patch")
			if err == nil {
				t.Fatal("invalid component graph admitted")
			}
			if strings.HasPrefix(mode, "After") && !strings.Contains(err.Error(), "only supported on the writer root") {
				t.Fatal(err)
			}
		})
	}
}
