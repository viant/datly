package writer

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestAuxiliaryCollectionsRejectNullEntries(t *testing.T) {
	for _, root := range []bool{true, false} {
		component := sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", "")
		target := component.RootView
		if !root {
			target = target.Relations[0].View
		}
		target.Auxiliary = true
		handler, err := New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
		if err != nil {
			t.Fatal(err)
		}
		input := &sqPlainInput{Rows: []*sqPlainParent{{ID: ptr(1), Has: &sqPlainParentHas{ID: true, Children: true}, Children: []*sqPlainChild{{ID: ptr(2), Has: &sqPlainChildHas{ID: true}}}}}}
		if _, err = handler.CaptureInput(context.Background(), input); err != nil {
			t.Fatal("ordinary auxiliary graph rejected:", err)
		}
		if root {
			input.Rows = append(input.Rows, nil)
		} else {
			input.Rows[0].Children = append(input.Rows[0].Children, nil)
		}
		if _, err = handler.CaptureInput(context.Background(), input); err == nil || !strings.Contains(err.Error(), "is nil") {
			t.Fatalf("root=%v: expected native null rejection, got %v", root, err)
		}
		if root {
			target.RootNullPolicy = "skip-auxiliary"
		} else {
			target.NestedNullPolicy = "skip-auxiliary"
		}
		if _, err = New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch"); err == nil {
			t.Fatal("removed policy admitted")
		}
	}
}
