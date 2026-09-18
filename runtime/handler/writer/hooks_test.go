package writer

import (
	"context"
	"reflect"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

type hookEntity struct{ ID *string }
type hookOutput struct{ Events []string }
type hookInput struct{ Rows []*hookEntity }

type hookProbe struct{}

func (*hookProbe) Init(_ context.Context, _ *hookEntity, state xhandler.LifecycleContext[hookEntity, xhandler.NoParent, hookOutput]) error {
	state.Output.Events = append(state.Output.Events, "init")
	return nil
}
func (*hookProbe) Validate(_ context.Context, _ *hookEntity, state xhandler.LifecycleContext[hookEntity, xhandler.NoParent, hookOutput]) error {
	state.Output.Events = append(state.Output.Events, "validate")
	return nil
}
func (*hookProbe) AfterSequence(_ context.Context, _ *hookEntity, state xhandler.LifecycleContext[hookEntity, xhandler.NoParent, hookOutput]) error {
	state.Output.Events = append(state.Output.Events, "afterSequence")
	return nil
}
func (*hookProbe) AfterQueue(_ context.Context, _ *hookEntity, state xhandler.LifecycleContext[hookEntity, xhandler.NoParent, hookOutput]) error {
	state.Output.Events = append(state.Output.Events, "afterQueue")
	return nil
}
func (*hookProbe) Finalize(_ context.Context, _ *hookInput, output *hookOutput, _ xhandler.Outcome) error {
	output.Events = append(output.Events, "finalize")
	return nil
}

func TestUniversalProgramInvokesTypedHooks(t *testing.T) {
	id := "one"
	entity := &hookEntity{ID: &id}
	output := &hookOutput{}
	program := &Program{input: &hookInput{Rows: []*hookEntity{entity}}, output: output, hook: reflect.ValueOf(&hookProbe{})}
	frame := &Frame{Entity: reflect.ValueOf(entity), Hook: reflect.ValueOf(&hookProbe{})}
	ctx := context.Background()
	for _, name := range []string{"Init", "Validate", "AfterSequence", "AfterQueue"} {
		if err := program.callEntityHook(ctx, name, frame); err != nil {
			t.Fatal(err)
		}
	}
	handler := &Handler{}
	if err := handler.FinalizeOutcome(ctx, rhandler.Invocation{Snapshot: program}, output, xhandler.Outcome{}); err != nil {
		t.Fatal(err)
	}
	want := []string{"init", "validate", "afterSequence", "afterQueue", "finalize"}
	if !reflect.DeepEqual(output.Events, want) {
		t.Fatalf("events = %v, want %v", output.Events, want)
	}
}
