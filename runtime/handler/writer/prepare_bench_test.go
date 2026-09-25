package writer

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

// fakeCapabilities supplies no-op validator, DML, sequencer and transaction
// starter so benchmarks measure the writer program itself.
type fakeCapabilities struct{ inserts, updates, deletes int }

func (f *fakeCapabilities) Bind(context.Context, any) error { return nil }
func (f *fakeCapabilities) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	switch key {
	case xhandler.FrameworkValidatorKey, xhandler.DMLKey, xhandler.SequencerKey, xhandler.TransactionStarterKey:
		return f, true, nil
	}
	return nil, false, nil
}
func (f *fakeCapabilities) Validate(context.Context, any, ...any) (*xhandler.Validation, error) {
	return &xhandler.Validation{}, nil
}
func (f *fakeCapabilities) Insert(string, any) error     { f.inserts++; return nil }
func (f *fakeCapabilities) Update(string, any) error     { f.updates++; return nil }
func (f *fakeCapabilities) Delete(string, any) error     { f.deletes++; return nil }
func (f *fakeCapabilities) Execute(string, ...any) error { return nil }
func (f *fakeCapabilities) UpdateWithOptions(string, any, ...xhandler.Option) error {
	f.updates++
	return nil
}
func (f *fakeCapabilities) DeleteWithOptions(string, any, ...xhandler.Option) error {
	f.deletes++
	return nil
}
func (f *fakeCapabilities) Allocate(context.Context, string, any, string) error { return nil }
func (f *fakeCapabilities) Start(context.Context) error                         { return nil }

// benchPatchInput builds parents×children rows plus a matching Current graph;
// every child changes its label so each frame produces a sparse update.
func benchPatchInput(parents, children int) *sqPlainInput {
	input := &sqPlainInput{}
	nextChild := 1
	for i := 1; i <= parents; i++ {
		parentID := i
		row := &sqPlainParent{ID: &parentID, Has: &sqPlainParentHas{ID: true, Children: true}}
		current := &sqPlainParent{ID: ptr(parentID), Name: ptr(fmt.Sprintf("parent-%d", i))}
		for j := 0; j < children; j++ {
			childID := nextChild
			nextChild++
			row.Children = append(row.Children, &sqPlainChild{ID: ptr(childID), Label: ptr("changed"), Has: &sqPlainChildHas{ID: true, Label: true}})
			input.CurrentChildren = append(input.CurrentChildren, &sqPlainChild{ID: ptr(childID), ParentID: ptr(parentID), Label: ptr("original")})
		}
		input.Rows = append(input.Rows, row)
		input.CurrentRows = append(input.CurrentRows, current)
	}
	return input
}

func benchPatchHandler(b *testing.B) *Handler {
	component := sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", "")
	handler, err := New(component, reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		b.Fatal(err)
	}
	return handler
}

func TestPreparePatchGraphProducesSparseChildUpdatesOnly(t *testing.T) {
	handler, err := New(sqComponent(reflect.TypeFor[sqPlainParent]().PkgPath(), "patch", ""), reflect.TypeFor[sqPlainInput](), reflect.TypeFor[sqPlainOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	capabilities := &fakeCapabilities{}
	input := benchPatchInput(20, 5)
	snapshot, err := handler.CaptureInput(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = handler.Execute(context.Background(), rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: capabilities}); err != nil {
		t.Fatal(err)
	}
	if capabilities.updates != 100 || capabilities.inserts != 0 || capabilities.deletes != 0 {
		t.Fatalf("writes = %+v, want 100 child updates only", *capabilities)
	}
}

func BenchmarkPreparePatchGraph(b *testing.B) {
	for _, shape := range []struct{ parents, children int }{{100, 5}, {1000, 5}, {2000, 10}} {
		b.Run(fmt.Sprintf("%dx%d", shape.parents, shape.children), func(b *testing.B) {
			b.ReportAllocs()
			handler := benchPatchHandler(b)
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				input := benchPatchInput(shape.parents, shape.children)
				b.StartTimer()
				snapshot, err := handler.CaptureInput(context.Background(), input)
				if err != nil {
					b.Fatal(err)
				}
				if _, err = handler.Execute(context.Background(), rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &fakeCapabilities{}}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
