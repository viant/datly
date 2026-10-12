package collector

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
)

type ordinaryRelationBenchmarkRow struct{ ID int }

// This keeps the original reader's first-non-hook exit and concurrency policy,
// using the same row accessor as the candidate to isolate relation dispatch.
func legacyRelationDispatch(ctx context.Context, collector *Collector) {
	ptr := xunsafe.AsPointer(collector.destValue.Interface())
	count := collector.slice.Len(ptr)
	concurrency := collector.view.Spec.RelationalConcurrency
	if concurrency <= 1 {
		for i := 0; i < count; i++ {
			if hook, ok := collector.slice.ValuePointerAt(ptr, i).(xhandler.OnRelationer); ok {
				hook.OnRelation(ctx)
				continue
			}
			break
		}
		return
	}
	slots := make(chan struct{}, concurrency)
	var workers sync.WaitGroup
	for i := 0; i < count; i++ {
		if hook, ok := collector.slice.ValuePointerAt(ptr, i).(xhandler.OnRelationer); ok {
			workers.Add(1)
			slots <- struct{}{}
			go func() {
				defer workers.Done()
				hook.OnRelation(ctx)
				<-slots
			}()
			continue
		}
		break
	}
	workers.Wait()
}

func BenchmarkRunOnRelationOrdinaryMillionRows(b *testing.B) {
	const count = 1_000_000
	for _, pointer := range []bool{false, true} {
		for _, concurrency := range []int{1, 8} {
			name := "value/sequential"
			if pointer {
				name = "pointer/sequential"
			}
			if concurrency > 1 {
				name = name[:len(name)-len("sequential")] + "concurrent"
			}
			b.Run(name, func(b *testing.B) {
				rows := make([]ordinaryRelationBenchmarkRow, count)
				for i := range rows {
					rows[i].ID = i + 1
				}
				rowType := reflect.TypeFor[ordinaryRelationBenchmarkRow]()
				var dest any = &rows
				if pointer {
					pointers := make([]*ordinaryRelationBenchmarkRow, count)
					for i := range pointers {
						pointers[i] = &rows[i]
					}
					rowType = reflect.PointerTo(rowType)
					dest = &pointers
				}
				view := newTestView(&data.View{Spec: spec.View{Name: "ordinary", RelationalConcurrency: concurrency}}, rowType)
				collector := NewCollector(view, dest, false)
				ctx := context.Background()
				for _, legacy := range []bool{true, false} {
					label := "v1"
					if legacy {
						label = "legacy-loop"
					}
					b.Run(label, func(b *testing.B) {
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if legacy {
								legacyRelationDispatch(ctx, collector)
							} else if err := collector.RunOnRelation(ctx); err != nil {
								b.Fatal(err)
							}
						}
					})
				}
			})
		}
	}
}
