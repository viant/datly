package builder

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/selectorerror"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqltemplate "github.com/viant/datly/sql/template"
)

type selectorOriginEvaluator struct{ failure error }

func (e selectorOriginEvaluator) Evaluate(context.Context, sqltemplate.Invocation) (sqltemplate.Result, error) {
	return sqltemplate.Result{}, e.failure
}
func TestNativeProjectionStageCannotBeRecycledByBuildCallbacks(t *testing.T) {
	ctx := t.Context()
	view := &data.View{Spec: spec.View{Name: "records"}}
	_, inner := NewBuilder().Build(ctx, WithBuilderView(view), WithBuilderSQL("SELECT id FROM records"), WithBuilderProjection([]string{"foreign"}))
	marker, ok := inner.(*selectorerror.Projection)
	if !ok || marker.View != view || marker.Cause == nil {
		t.Fatalf("native projection origin missing: %T", inner)
	}
	native := marker.Cause
	for _, wrap := range []string{"direct", "wrapped", "joined"} {
		var returned error = inner
		if wrap == "wrapped" {
			returned = fmt.Errorf("callback: %w", inner)
		}
		if wrap == "joined" {
			returned = errors.Join(inner, errors.New("private second callback error"))
		}
		for _, exit := range []string{"template-before-projection", "resolver-after-projection"} {
			t.Run(wrap+"/"+exit, func(t *testing.T) {
				calls := 0
				opts := []BuilderOption{WithBuilderView(view), WithBuilderSQL("SELECT id FROM records WHERE id=:ID")}
				if exit == "template-before-projection" {
					opts = append(opts, WithBuilderTemplate(selectorOriginEvaluator{failure: returned}))
				} else {
					opts = append(opts, WithBuilderParameterResolver(func(string) (any, bool, error) { calls++; return nil, false, returned }))
				}
				_, failure := NewBuilder().Build(ctx, opts...)
				if failure == nil || !errors.Is(failure, native) {
					t.Fatalf("callback native cause lost: %T", failure)
				}
				if _, ok := failure.(*selectorerror.Projection); ok {
					t.Fatal("foreign direct stage authority escaped Build")
				}
				if wrap == "direct" && exit == "template-before-projection" && failure != native {
					t.Fatal("direct foreign marker did not retain exact original cause")
				}
				if exit == "resolver-after-projection" && calls == 0 {
					t.Fatal("resolver exit was not reached")
				}
			})
		}
	}
	_, again := NewBuilder().Build(ctx, WithBuilderView(view), WithBuilderSQL("SELECT id FROM records"), WithBuilderProjection([]string{"foreign"}))
	next, ok := again.(*selectorerror.Projection)
	if !ok || next == marker || next.View != view {
		t.Fatal("native stage identity must be fresh per Build")
	}
	var unknown *dsql.UnknownProjectionColumnError
	if !errors.As(next, &unknown) || unknown != next.Cause || next.Error() != unknown.Error() {
		t.Fatal("native stage changed original cause/text")
	}
}
