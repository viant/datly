package reader

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	viewcontext "github.com/viant/datly/internal/viewcontext"
	"github.com/viant/datly/spec"
	rcollector "github.com/viant/datly/sql/reader/collector"
	xhandler "github.com/viant/xdatly/handler"
)

type contextParent struct{ ID int }
type contextInput struct{ ID int }

// Use an actual collector owner; parent resolution itself is covered by the
// mapped-key SQLite/cache replay tests. This isolates hook-context ownership.
func contextVisitor(base context.Context) *rowHookVisitor {
	childView := &rcollector.View{View: &data.View{Spec: spec.View{PublishParent: true}}, Schema: rcollector.NewSchema(reflect.TypeFor[*contextParent]())}
	parentView := &rcollector.View{View: &data.View{}, Schema: rcollector.NewSchema(reflect.TypeFor[*contextParent]())}
	parentView.Relations = []*rcollector.Relation{{Relation: &data.Relation{Holder: "Children"}, Of: &rcollector.RelationRef{RelationRef: &data.RelationRef{}, View: childView}}}
	var dest []*contextParent
	parent := rcollector.NewCollector(parentView, &dest, false)
	child := parent.Relations(nil)[0]
	visitor := newRowHookVisitor(base, childView.View, child, nil)
	visitor.parentProvider = func(row any) (any, error) { return row, nil }
	return visitor
}

func TestParentHookContextIsVisitorOwnedAndPreservesInheritedValues(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	input := &contextInput{ID: 7}
	outerParent := &contextParent{ID: 99}
	outer := viewcontext.NewContext(base)
	outer.WithValue(xhandler.InputKey, input)
	outer.WithValue(reflect.TypeFor[*contextInput](), input)
	outer.WithValue(reflect.TypeFor[*contextParent](), outerParent)
	visitor := contextVisitor(outer)
	first := &contextParent{ID: 1}
	second := &contextParent{ID: 2}
	ctx, err := visitor.hookContext(first)
	require.NoError(t, err)
	require.Same(t, first, ctx.Value(reflect.TypeFor[*contextParent]()))
	require.Same(t, input, ctx.Value(xhandler.InputKey))
	require.Same(t, input, ctx.Value(reflect.TypeFor[*contextInput]()))
	require.Same(t, visitor.collector.Parent().DataSync(), ctx.Value(xhandler.DataSyncKey))
	require.Same(t, outerParent, outer.Value(reflect.TypeFor[*contextParent]()), "never mutate an incoming reusable context")
	next, err := visitor.hookContext(second)
	require.NoError(t, err)
	require.Same(t, ctx, next, "reuse the original mutable context semantics within one visitor")
	require.Same(t, second, ctx.Value(reflect.TypeFor[*contextParent]()))
	require.Same(t, outerParent, outer.Value(reflect.TypeFor[*contextParent]()))
	cancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Equal(t, base.Done(), ctx.Done())
}

func TestParentHookContextsStaySeparateAcrossConcurrentVisitors(t *testing.T) {
	base := viewcontext.NewContext(context.Background())
	input := &contextInput{ID: 7}
	base.WithValue(xhandler.InputKey, input)
	const workers = 8
	visitors := make([]*rowHookVisitor, workers)
	for i := range visitors {
		visitors[i] = contextVisitor(base)
	}
	failures := make(chan error, workers)
	var group sync.WaitGroup
	for i, visitor := range visitors {
		group.Add(1)
		go func() {
			defer group.Done()
			parent := &contextParent{ID: i}
			for range 10000 {
				ctx, err := visitor.hookContext(parent)
				if err != nil {
					failures <- err
					return
				}
				if ctx.Value(reflect.TypeFor[*contextParent]()) != parent || ctx.Value(xhandler.InputKey) != input {
					failures <- errors.New("context escaped its visitor owner")
					return
				}
			}
		}()
	}
	group.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	require.Nil(t, base.Value(reflect.TypeFor[*contextParent]()))
}

func TestParentHookContextHasNoSteadyStateRowAllocations(t *testing.T) {
	visitor := contextVisitor(context.Background())
	parent := &contextParent{ID: 1}
	_, err := visitor.hookContext(parent)
	require.NoError(t, err)
	allocations := testing.AllocsPerRun(1000, func() {
		ctx, err := visitor.hookContext(parent)
		if err != nil || ctx.Value(reflect.TypeFor[*contextParent]()) != parent {
			panic("invalid parent context")
		}
	})
	require.Zero(t, allocations)
}

func TestParentHookContextNilAndResolutionFailureDoNotPublishStaleParent(t *testing.T) {
	base := context.Background()
	visitor := contextVisitor(base)
	_, err := visitor.hookContext(&contextParent{})
	require.NoError(t, err)
	ctx, err := visitor.hookContext(nil)
	require.NoError(t, err)
	require.Equal(t, base, ctx)
	failure := errors.New("ambiguous parent")
	visitor.parentProvider = func(any) (any, error) { return nil, failure }
	ctx, err = visitor.hookContext(&contextParent{})
	require.ErrorIs(t, err, failure)
	require.Nil(t, ctx)
	visitor.view.Spec.PublishParent = false
	ctx, err = visitor.hookContext(&contextParent{})
	require.NoError(t, err)
	require.Equal(t, base, ctx)
}

var parentContextBenchmarkSink context.Context

func BenchmarkParentAwareHookContext(b *testing.B) {
	visitor := contextVisitor(context.Background())
	parents := [2]*contextParent{{ID: 1}, {ID: 2}}
	if _, err := visitor.hookContext(parents[0]); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, err := visitor.hookContext(parents[i&1])
		if err != nil {
			b.Fatal(err)
		}
		parentContextBenchmarkSink = ctx
	}
}
