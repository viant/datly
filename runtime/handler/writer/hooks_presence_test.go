package writer

import (
	"context"
	"reflect"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

// Keep hook types linked so xunsafe can resolve them by name.
var _ = []reflect.Type{reflect.TypeOf(sqServerDefaultHooks{}), reflect.TypeOf(sqWithdrawHooks{})}

// sqServerDefaultHooks assigns a server-owned value during Init, the phase
// for marker-aware business defaults, and marks it present.
type sqServerDefaultHooks struct{}

func (*sqServerDefaultHooks) Init(_ context.Context, entity *sqPlainParent, _ xhandler.LifecycleContext[sqPlainParent, xhandler.NoParent, sqPlainOutput]) error {
	name := "server-default"
	entity.Name = &name
	if entity.Has == nil {
		entity.Has = &sqPlainParentHas{}
	}
	entity.Has.Name = true
	return nil
}

// sqWithdrawHooks withdraws a client-supplied field during Init (for example
// because the caller lacks permission) while leaving the value in place.
type sqWithdrawHooks struct{}

func (*sqWithdrawHooks) Init(_ context.Context, entity *sqPlainParent, _ xhandler.LifecycleContext[sqPlainParent, xhandler.NoParent, sqPlainOutput]) error {
	if entity.Has != nil {
		entity.Has.Name = false
	}
	return nil
}

func runPlainWithHooks(t *testing.T, operation, hooks string, input *sqPlainInput) *fakeCapabilities {
	t.Helper()
	component := sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), operation, hooks)
	handler, err := New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), operation)
	if err != nil {
		t.Fatal(err)
	}
	if handler.metadata.HookType != reflect.TypeOf(sqServerDefaultHooks{}) && handler.metadata.HookType != reflect.TypeOf(sqWithdrawHooks{}) {
		t.Fatalf("hook type %s was not resolved: %v", hooks, handler.metadata.HookType)
	}
	capabilities := &fakeCapabilities{}
	snapshot, err := handler.CaptureInput(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = handler.Execute(context.Background(), rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: capabilities}); err != nil {
		t.Fatal(err)
	}
	return capabilities
}

// A marker set by a hook is visible to every later phase without any
// synchronisation: the client sent an identity-only row, Init supplied Name,
// and both validation passes report Name present and the row is written.
func TestPresenceReflectsMarkerSetInHook(t *testing.T) {
	current := &sqPlainParent{ID: ptr(1), Name: ptr("original")}
	row := &sqPlainParent{ID: ptr(1), Has: &sqPlainParentHas{ID: true}}
	capabilities := runPlainWithHooks(t, "patch", "sqServerDefaultHooks", &sqPlainInput{Rows: []*sqPlainParent{row}, CurrentRows: []*sqPlainParent{current}})
	if len(capabilities.validations) != 2 {
		t.Fatalf("expected two validation passes, got %d", len(capabilities.validations))
	}
	for pass, options := range capabilities.validations {
		if len(options) != 1 || options[0].Fields == nil || !options[0].Fields.Has("Name") {
			t.Fatalf("pass %d did not see Name supplied by Init: %+v", pass, options)
		}
	}
	if capabilities.updates != 1 || row.Name == nil || *row.Name != "server-default" {
		t.Fatalf("updates=%d name=%v", capabilities.updates, row.Name)
	}
}

// A marker cleared by Init withdraws the field: the update becomes a no-op,
// no validation pass reports the field, and the original snapshot still
// records that the client supplied it.
func TestPresenceHonoursMarkerClearedByInit(t *testing.T) {
	current := &sqPlainParent{ID: ptr(1), Name: ptr("original")}
	row := &sqPlainParent{ID: ptr(1), Name: ptr("forbidden"), Has: &sqPlainParentHas{ID: true, Name: true}}
	capabilities := runPlainWithHooks(t, "patch", "sqWithdrawHooks", &sqPlainInput{Rows: []*sqPlainParent{row}, CurrentRows: []*sqPlainParent{current}})
	if capabilities.updates != 0 || capabilities.inserts != 0 {
		t.Fatalf("withdrawn field must not produce writes: %+v", capabilities)
	}
	for pass, options := range capabilities.validations {
		if len(options) != 0 {
			t.Fatalf("pass %d validated a frame that has no mutable fields: %+v", pass, options)
		}
	}
	if row.Has.Name {
		t.Fatal("Init withdrawal was undone")
	}
}
