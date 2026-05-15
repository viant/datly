package state

import (
	"reflect"
	"testing"

	"github.com/viant/structology"
)

func TestParameters_SetLiterals_DoesNotReuseSelectorAcrossStateTypes(t *testing.T) {
	const (
		paramName   = "X"
		dummyName   = "Dummy"
		dummyValue  = 12345
		constValue  = true
		constSource = "value"
	)

	param := &Parameter{
		Name:  paramName,
		In:    &Location{Kind: KindConst, Name: constSource},
		Value: constValue,
		Schema: &Schema{
			rType: reflect.TypeOf(true),
		},
	}
	params := Parameters{param}

	type1 := reflect.StructOf([]reflect.StructField{
		{Name: dummyName, Type: reflect.TypeOf(int(0))},
		{Name: paramName, Type: reflect.TypeOf(true)},
	})
	state1 := structology.NewStateType(type1).NewState()
	if err := state1.SetInt(dummyName, dummyValue); err != nil {
		t.Fatalf("failed to init %s: %v", dummyName, err)
	}
	if err := params.SetLiterals(state1); err != nil {
		t.Fatalf("SetLiterals(type1) failed: %v", err)
	}
	if got, err := state1.Bool(paramName); err != nil || got != constValue {
		t.Fatalf("type1 %s: got=%v err=%v, want=%v", paramName, got, err, constValue)
	}
	if got, err := state1.Value(dummyName); err != nil || got.(int) != dummyValue {
		t.Fatalf("type1 %s: got=%v err=%v, want=%v", dummyName, got, err, dummyValue)
	}

	type2 := reflect.StructOf([]reflect.StructField{
		{Name: paramName, Type: reflect.TypeOf(true)},
		{Name: dummyName, Type: reflect.TypeOf(int(0))},
	})
	state2 := structology.NewStateType(type2).NewState()
	if err := state2.SetInt(dummyName, dummyValue); err != nil {
		t.Fatalf("failed to init %s: %v", dummyName, err)
	}
	if err := params.SetLiterals(state2); err != nil {
		t.Fatalf("SetLiterals(type2) failed: %v", err)
	}
	if got, err := state2.Bool(paramName); err != nil || got != constValue {
		t.Fatalf("type2 %s: got=%v err=%v, want=%v", paramName, got, err, constValue)
	}
	if got, err := state2.Value(dummyName); err != nil || got.(int) != dummyValue {
		t.Fatalf("type2 %s: got=%v err=%v, want=%v", dummyName, got, err, dummyValue)
	}
}
