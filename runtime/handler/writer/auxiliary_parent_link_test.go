package writer

import (
	"reflect"
	"testing"
)

func TestAuxiliaryUnresolvedParentLink(t *testing.T) {
	type useCase struct {
		description string
		auxiliary   bool
		parentID    int
		wantError   bool
	}
	for _, tc := range []useCase{
		{"lookup link can be initialized", true, 0, false},
		{"lookup resolved mismatch stays rejected", true, 2, true},
		{"writable child missing owner stays rejected", false, 0, true},
		{"lookup matched owner passes", true, 1, false},
	} {
		t.Run(tc.description, func(t *testing.T) {
			component := sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", "")
			component.RootView.Relations[0].View.Auxiliary = tc.auxiliary
			handler, err := New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			input := &sqPlainInput{
				Rows:            []*sqPlainParent{{ID: ptr(tc.parentID), Children: []*sqPlainChild{{ID: ptr(10)}}}},
				CurrentChildren: []*sqPlainChild{{ID: ptr(10), ParentID: ptr(1)}},
			}
			snapshot, err := handler.CaptureInput(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			program := snapshot.(*Program)
			if err = program.indexRecordCurrent(reflect.ValueOf(input).Elem(), program.metadata.Root); err != nil {
				t.Fatal(err)
			}
			err = program.buildRecordFrames(t.Context(), nil, program.metadata.Root, reflect.ValueOf(input.Rows), nil)
			gotError := err != nil || program.parentScopeError != nil
			if gotError != tc.wantError {
				t.Fatalf("error=%v scope=%v, want error=%v", err, program.parentScopeError, tc.wantError)
			}
			if tc.auxiliary && tc.parentID == 0 {
				// Rebuilding after Init must still catch an incorrect resolved link.
				input.Rows[0].ID = ptr(2)
				_ = program.buildRecordFrames(t.Context(), nil, program.metadata.Root, reflect.ValueOf(input.Rows), nil)
				if program.parentScopeError == nil {
					t.Fatal("resolved mismatch escaped the second framing pass")
				}
			}
		})
	}
}
