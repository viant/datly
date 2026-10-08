package writer

import (
	"reflect"
	"strings"
	"testing"
)

func TestImmutableValuesNestedInvocationLogger(t *testing.T) {
	type runtimeLogger struct{ Emit func() }
	type componentOutput struct {
		OutputLogger *runtimeLogger `parameter:"OutputLogger,kind=logger,in=" json:"-"`
		Context      struct{ AccountID int }
	}
	value := &componentOutput{OutputLogger: &runtimeLogger{Emit: func() {}}}
	value.Context.AccountID = 1
	before, err := immutableValues([]reflect.Value{reflect.ValueOf(value)})
	if err != nil {
		t.Fatal(err)
	}
	value.Context.AccountID = 2
	after, err := immutableValues([]reflect.Value{reflect.ValueOf(value)})
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("nested component contract mutation was not detected")
	}
	_, err = immutableValues([]reflect.Value{reflect.ValueOf(struct{ Callback func() }{func() {}})})
	if err == nil || !strings.Contains(err.Error(), "Callback") {
		t.Fatalf("ordinary function data must remain fail-closed with field context: %v", err)
	}
}
