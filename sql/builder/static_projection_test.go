package builder

import (
	"context"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/viant/datly/data"
	readerpredicate "github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqltemplate "github.com/viant/datly/sql/template"
	xhandler "github.com/viant/xdatly/handler"
)

type staticCountingEvaluator struct {
	calls  atomic.Int64
	native sqltemplate.Evaluator
}

func (e *staticCountingEvaluator) Evaluate(ctx context.Context, in sqltemplate.Invocation) (sqltemplate.Result, error) {
	e.calls.Add(1)
	return e.native.Evaluate(ctx, in)
}

type staticTestBinder struct{ input any }

func (b staticTestBinder) Bind(context.Context, any) error { return nil }
func (b staticTestBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	return b.input, key == xhandler.InputKey, nil
}

func TestBuilderConsumesCheckedSelectionAfterOneEvaluation(t *testing.T) {
	type input struct{}
	source := `SELECT records.* FROM (SELECT r.ID,r.NAME FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(99,"AND")).Build("WHERE")}) records`
	view := &data.View{Spec: spec.View{Source: &spec.ViewSource{SQL: source}}, Columns: []*data.Column{{Name: "ID", Column: "ID"}, {Name: "NAME", Column: "NAME"}}}
	plan, err := dsql.NewStaticProjection(source, view, nil)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := plan.Select([]string{"NAME"})
	if err != nil {
		t.Fatal(err)
	}
	predicate, err := readerpredicate.Compile(readerpredicate.CompileInput{})
	if err != nil {
		t.Fatal(err)
	}
	native, err := (sqltemplate.Compiler{Source: source, InputType: reflect.TypeFor[input](), Predicate: predicate}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	evaluated := &staticCountingEvaluator{native: native}
	actual := input{}
	opts := []BuilderOption{WithBuilderView(view), WithBuilderTemplate(evaluated), WithBuilderInput(reflect.ValueOf(actual)), WithBuilderBinder(staticTestBinder{input: &actual}), WithBuilderProjection([]string{"NAME"}), WithBuilderStaticProjection(selected, evaluated)}
	result, err := NewBuilder().Build(t.Context(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	if evaluated.calls.Load() != 1 || !strings.HasPrefix(result.SQL, "SELECT NAME FROM (") || strings.Contains(result.SQL, "${predicate") || strings.Contains(result.SQL, "__datly_analysis_") {
		t.Fatalf("checked source rendering/evaluation mismatch: calls=%d SQL=%s", evaluated.calls.Load(), result.SQL)
	}
	for _, tc := range []struct {
		name     string
		override BuilderOption
	}{
		{"different-fields", WithBuilderProjection([]string{"ID"})},
		{"explicit-sql-even-same-text", WithBuilderSQL(source)},
		{"different-evaluator", WithBuilderTemplate(&staticCountingEvaluator{native: native})},
		{"different-view", WithBuilderView(&data.View{Spec: view.Spec, Columns: view.Columns})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := evaluated.calls.Load()
			invalid := append(append([]BuilderOption(nil), opts...), tc.override)
			if _, err := NewBuilder().Build(t.Context(), invalid...); err == nil {
				t.Fatal("foreign execution owner admitted")
			}
			if evaluated.calls.Load() != before {
				t.Fatal("template evaluated before owner rejection")
			}
		})
	}
	if _, err = plan.Select([]string{"missing"}); err == nil {
		t.Fatal("unknown field admitted")
	}
	if evaluated.calls.Load() != 1 {
		t.Fatal("field validation evaluated the template")
	}
}
