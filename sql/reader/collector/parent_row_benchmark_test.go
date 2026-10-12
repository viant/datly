package collector_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/reader/collector"
)

type parent struct {
	ID       int
	Children []*child
}
type child struct{ ID, ParentID int }

// Exercise the actual published collector parent resolver with one million
// distinct parents and child keys. Setup/index construction is outside timing;
// each timed lookup must return the exact stored parent pointer. This is a
// resolver benchmark, not an SQL or complete OnFetch throughput claim.
func BenchmarkParentRowMillionKeys(b *testing.B) {
	const count = 1_000_000
	for _, pointerParent := range []bool{false, true} {
		name := "value-parent"
		if pointerParent {
			name = "pointer-parent"
		}
		b.Run(name, func(b *testing.B) {
			metadataChild := &data.View{Spec: spec.View{Name: "children", PublishParent: true}}
			metadataParent := &data.View{Spec: spec.View{Name: "parents"}, Relations: []*data.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, On: data.Links{data.NewLink("", "ID", "ID")}, Of: &data.RelationRef{View: metadataChild, On: data.Links{data.NewLink("", "ParentID", "ParentID")}}}}}
			parentType := reflect.TypeFor[parent]()
			if pointerParent {
				parentType = reflect.PointerTo(parentType)
			}
			graph, err := collector.Compile(metadataParent, map[*data.View]reflect.Type{metadataParent: parentType, metadataChild: reflect.TypeFor[*child]()})
			if err != nil {
				b.Fatal(err)
			}
			values := make([]parent, count)
			children := make([]child, count)
			expected := make([]*parent, count)
			for i := range values {
				values[i].ID = i + 1
				children[i] = child{ID: i + 1, ParentID: i + 1}
				expected[i] = &values[i]
			}
			var dest any
			var rows any
			if pointerParent {
				var result []*parent
				dest = &result
				rows = expected
			} else {
				var result []parent
				dest = &result
				rows = values
			}
			root := collector.NewCollector(graph.Root, dest, false)
			if err = root.AppendSlice(context.Background(), rows); err != nil {
				b.Fatal(err)
			}
			root.Fetched()
			if !pointerParent {
				stored := root.Dest().([]parent)
				for i := range stored {
					expected[i] = &stored[i]
				}
			}
			resolve := root.Relations(nil)[0].ParentRow()
			if resolve == nil {
				b.Fatal("missing parent resolver")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				i := n % count
				actual, err := resolve(&children[i])
				if err != nil {
					b.Fatal(err)
				}
				if actual != expected[i] {
					b.Fatalf("child %d resolved a different parent", i+1)
				}
			}
			b.StopTimer()
			b.ReportMetric(count, "parents")
			b.ReportMetric(float64(b.N), "verified-lookups")
		})
	}
}

// A single public resolver remains safe for simultaneous first lookups after
// the parent fetch is complete, as required by actual row visitors.
func TestParentRowConcurrentFirstLookups(t *testing.T) {
	childView := &data.View{Spec: spec.View{Name: "children"}}
	parentView := &data.View{Spec: spec.View{Name: "parents"}, Relations: []*data.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, On: data.Links{data.NewLink("", "ID", "ID")}, Of: &data.RelationRef{View: childView, On: data.Links{data.NewLink("", "ParentID", "ParentID")}}}}}
	graph, err := collector.Compile(parentView, map[*data.View]reflect.Type{parentView: reflect.TypeFor[*parent](), childView: reflect.TypeFor[*child]()})
	if err != nil {
		t.Fatal(err)
	}
	var dest []*parent
	root := collector.NewCollector(graph.Root, &dest, false)
	first := &parent{ID: 1}
	if err = root.AppendSlice(context.Background(), []*parent{first}); err != nil {
		t.Fatal(err)
	}
	root.Fetched()
	resolver := root.Relations(nil)[0].ParentRow()
	var group sync.WaitGroup
	errors := make(chan error, 8)
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 10000 {
				value, err := resolver(&child{ParentID: 1})
				if err != nil {
					errors <- err
					return
				}
				if value != first {
					errors <- fmt.Errorf("wrong parent pointer")
					return
				}
			}
		}()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}
