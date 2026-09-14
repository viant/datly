package reader

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

func TestExecutionCreatesFreshInvocationSession(t *testing.T) {
	type input struct{}
	type output struct{ Data any }
	component := &spec.Component{
		RootView: &spec.View{Source: &spec.ViewSource{SQL: "SELECT 1"}},
	}
	view := &data.View{Spec: spec.View{Source: &spec.ViewSource{SQL: "SELECT 1"}}}
	plan, err := NewPlan(PlanConfig{
		RootView: view, ViewIndex: NewViewIndex(component, view),
	})
	if err != nil {
		t.Fatalf("new reader plan failed: %v", err)
	}
	execution, err := NewExecution(Config{
		Component:  component,
		InputType:  reflect.TypeOf(input{}),
		OutputType: reflect.TypeOf(output{}),
		Plan:       plan,
		SQL:        &dsql.SQLComponent{},
	})
	if err != nil {
		t.Fatalf("new execution failed: %v", err)
	}

	first := execution.session()
	second := execution.session()
	if first == second {
		t.Fatal("reader execution must allocate invocation-local sessions")
	}
	first.Data = "first"
	if second.Data != nil {
		t.Fatalf("reader output state leaked across invocations: %#v", second.Data)
	}
}

func TestExecutionRejectsIncompletePlan(t *testing.T) {
	component := &spec.Component{RootView: &spec.View{Source: &spec.ViewSource{SQL: "SELECT 1"}}}
	_, err := NewExecution(Config{
		Component: component,
		InputType: reflect.TypeOf(struct{}{}),
		Plan:      &Plan{},
		SQL:       &dsql.SQLComponent{},
	})
	if err == nil || !strings.Contains(err.Error(), "root view is required") {
		t.Fatalf("expected incomplete plan error, got %v", err)
	}
}

func TestNilExecutionReturnsError(t *testing.T) {
	var execution *Execution
	if _, err := execution.Read(context.Background(), &struct{}{}, nil, nil); err == nil {
		t.Fatal("expected nil execution error")
	}
}
