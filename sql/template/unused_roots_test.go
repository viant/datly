package template

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

type templateStatus struct{ Message string }
type templateReply struct {
	templateStatus
	Message string
}

func TestCompilerUnusedCollidingRootPreservesCanonicalInput(t *testing.T) {
	type input struct {
		Reply *templateReply
		IDs   []int
	}
	program, err := (Compiler{Source: `SELECT * FROM records WHERE $criteria.In("id", $IDs)`, InputType: reflect.TypeFor[input](), Variables: []Variable{{Name: "AliasReply", FieldIndex: []int{0}}}}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][]int{{1, 2}, nil, {3}} {
		result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{IDs: ids})})
		if err != nil {
			t.Fatal(err)
		}
		var want []any
		for _, id := range ids {
			want = append(want, id)
		}
		if !reflect.DeepEqual(result.Args, want) {
			t.Fatalf("fresh arguments: %+v for %v", result, ids)
		}
		if ids == nil && strings.TrimSpace(result.SQL) != "SELECT * FROM records WHERE 1 = 0" {
			t.Fatalf("nil IDs predicate: %q", result.SQL)
		}
	}
}

func TestCompilerReferencedCollidingRootStillFails(t *testing.T) {
	type input struct{ Reply *templateReply }
	for _, source := range []string{`#if($Reply.Message) SELECT 1 #end`, `#if($Unsafe.Reply.Message) SELECT 1 #end`, `#if($AliasReply.Message) SELECT 1 #end`} {
		_, err := (Compiler{Source: source, InputType: reflect.TypeFor[input](), Variables: []Variable{{Name: "AliasReply", FieldIndex: []int{0}}}}).Compile()
		if err == nil || !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("referenced collision %q: %v", source, err)
		}
	}
}

func TestCompilerDynamicEvaluationRetainsInheritedInput(t *testing.T) {
	type input struct {
		Script string
		Value  int
	}
	program, err := (Compiler{Source: `#if(true) #evaluate($Script) #end`, InputType: reflect.TypeFor[input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{Script: `SELECT $Unsafe.Value`, Value: 7})})
	if err != nil || !strings.Contains(result.SQL, "SELECT 7") {
		t.Fatalf("inherited input: %q %v", result.SQL, err)
	}
}

func TestCompilerLoopRetainsInitializerAndPostInputs(t *testing.T) {
	type input struct {
		Start int
		Step  int
		End   int
	}
	program, err := (Compiler{Source: `#for($i = $Start; $i < $End; $i = $i + $Step) SELECT $i; #end`, InputType: reflect.TypeFor[input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{Start: 1, Step: 2, End: 4})})
	if err != nil || !reflect.DeepEqual(result.Args, []any{1, 3}) {
		t.Fatalf("loop bindings: %+v %v", result, err)
	}
}
