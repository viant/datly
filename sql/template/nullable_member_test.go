package template

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/viant/velty/ast/expr"
)

type nullableTemplateBounds struct {
	RecordedAt *time.Time
	private    *time.Time
}
type nullableTemplateInput struct{ Bounds *nullableTemplateBounds }

func nullableTemplateProgram(t *testing.T) Evaluator {
	t.Helper()
	program, err := (Compiler{Source: "SELECT $Bounds.RecordedAt", InputType: reflect.TypeFor[nullableTemplateInput](),
		Variables: []Variable{{Name: "Bounds", FieldIndex: []int{0}}}}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	return program
}

func TestCompiledNullableMemberKeepsNullAcrossEvaluations(t *testing.T) {
	program := nullableTemplateProgram(t)
	value := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, input := range []nullableTemplateInput{{}, {Bounds: &nullableTemplateBounds{}}, {Bounds: &nullableTemplateBounds{RecordedAt: &value}}, {}} {
		actual, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input)})
		if err != nil {
			t.Fatal(err)
		}
		if len(actual.Args) != 1 {
			t.Fatalf("argument count=%d", len(actual.Args))
		}
		date, ok := actual.Args[0].(*time.Time)
		if !ok {
			t.Fatalf("date type lost: %T", actual.Args[0])
		}
		if input.Bounds == nil || input.Bounds.RecordedAt == nil {
			if date != nil {
				t.Fatal("null date became a value")
			}
		} else if date == nil || !date.Equal(value) {
			t.Fatal("existing date changed")
		}
	}
}

func TestCompiledNullableMemberConcurrentInvocations(t *testing.T) {
	program := nullableTemplateProgram(t)
	var wait sync.WaitGroup
	for i := 0; i < 16; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			input := nullableTemplateInput{}
			for j := 0; j < 8; j++ {
				actual, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input)})
				if err != nil || len(actual.Args) != 1 {
					t.Error("concurrent nullable evaluation failed")
					return
				}
				date, ok := actual.Args[0].(*time.Time)
				if !ok || date != nil || input.Bounds != nil {
					t.Error("nullable invocation retained mutable state")
					return
				}
			}
		}()
	}
	wait.Wait()
}

func TestNullableMemberTypeDoesNotExtendVisibility(t *testing.T) {
	root := reflect.TypeFor[*nullableTemplateBounds]()
	for _, name := range []string{"private", "Missing"} {
		if nullableMemberType(root, &expr.Select{ID: name}) != nil {
			t.Fatal("unavailable member received type metadata")
		}
	}
	if nullableMemberType(root, &expr.Call{X: &expr.Select{ID: "RecordedAt"}}) != nil {
		t.Fatal("call expression received member metadata")
	}
	if nullableMemberType(reflect.TypeFor[any](), &expr.Select{ID: "RecordedAt"}) != nil {
		t.Fatal("unknown type received member metadata")
	}
	if _, err := (Compiler{Source: "SELECT $Bounds.Missing", InputType: reflect.TypeFor[nullableTemplateInput](), Variables: []Variable{{Name: "Bounds", FieldIndex: []int{0}}}}).Compile(); err == nil {
		t.Fatal("missing member unexpectedly compiled")
	}
}
