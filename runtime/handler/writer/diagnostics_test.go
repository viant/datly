package writer

import (
	"context"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type diagnosticChild struct{ Name string }
type diagnosticParent struct {
	Children []*diagnosticChild
	One      *diagnosticChild
}
type diagnosticInput struct{ Payload []*diagnosticParent }

type locationHook struct{ actual string }

func (hook *locationHook) Validate(_ context.Context, _ *diagnosticChild, state xhandler.LifecycleContext[diagnosticChild, diagnosticParent, struct{}]) error {
	hook.actual = state.Location
	return nil
}

func TestWriterCanonicalIndexedRowLocations(t *testing.T) {
	root := &Record{Path: "query_role", EntityType: reflect.TypeFor[diagnosticParent]()}
	child := &Record{Path: "query_role.child_alias", EntityType: reflect.TypeFor[diagnosticChild]()}
	one := &Record{Path: "query_role.one_alias", EntityType: reflect.TypeFor[diagnosticChild]()}
	root.Relations = []*Relation{{Child: child, Field: []int{0}}, {Child: one, Field: []int{1}}}
	input := &diagnosticInput{Payload: []*diagnosticParent{{Children: []*diagnosticChild{{Name: "first"}}}, {Children: []*diagnosticChild{{Name: "second"}}, One: &diagnosticChild{Name: "one"}}}}
	program := &Program{metadata: &Metadata{InputField: 0, Operation: "post"}, input: input, original: &OriginalInput{Presence: map[uintptr]originalPresence{}}, database: &DatabaseSnapshot{Rows: map[rowIdentity]reflect.Value{}}, frames: &MutationFrames{}}
	if err := program.buildRecordFrames(context.Background(), nil, root, reflect.ValueOf(input.Payload), nil); err != nil {
		t.Fatal(err)
	}
	expected := []string{"Payload[0]", "Payload[0].Children[0]", "Payload[1]", "Payload[1].Children[0]", "Payload[1].One"}
	var actual []string
	for _, frame := range program.frames.Rows {
		actual = append(actual, frame.Location)
		if program.validationOptions(frame, false).Location != frame.Location {
			t.Fatal("validator location differs from row location")
		}
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected=%v,actual=%v", expected, actual)
	}
	hook := &locationHook{}
	frame := program.frames.Rows[3]
	frame.Hook = reflect.ValueOf(hook)
	program.output = &struct{}{}
	if err := program.callEntityHook(context.Background(), "Validate", frame); err != nil {
		t.Fatal(err)
	}
	if hook.actual != expected[3] {
		t.Fatalf("hook row location=%q,want=%q", hook.actual, expected[3])
	}
}
