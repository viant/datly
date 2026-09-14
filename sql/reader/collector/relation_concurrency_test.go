package collector

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

type relationConcurrencyProbe struct {
	active  atomic.Int32
	maximum atomic.Int32
	calls   atomic.Int32
}

type relationConcurrencyRow struct {
	probe *relationConcurrencyProbe
}

func (r *relationConcurrencyRow) OnRelation(context.Context) {
	active := r.probe.active.Add(1)
	for maximum := r.probe.maximum.Load(); active > maximum && !r.probe.maximum.CompareAndSwap(maximum, active); maximum = r.probe.maximum.Load() {
	}
	r.probe.calls.Add(1)
	time.Sleep(20 * time.Millisecond)
	r.probe.active.Add(-1)
}

func TestCollector_RunOnRelationUsesBoundedWorkers(t *testing.T) {
	probe := &relationConcurrencyProbe{}
	rType := reflect.TypeOf(relationConcurrencyRow{})
	view := newTestView(&data.View{Spec: spec.View{RelationalConcurrency: 2}}, rType)
	dest := &[]relationConcurrencyRow{}
	collector := NewCollector(view, dest, false)
	for i := 0; i < 5; i++ {
		collector.NewItem()().(*relationConcurrencyRow).probe = probe
	}
	collector.RunOnRelation(context.Background())
	if calls := probe.calls.Load(); calls != 5 {
		t.Fatalf("unexpected hook count: %d", calls)
	}
	if maximum := probe.maximum.Load(); maximum != 2 {
		t.Fatalf("unexpected maximum concurrency: %d", maximum)
	}
	if active := probe.active.Load(); active != 0 {
		t.Fatalf("hook workers remained active: %d", active)
	}
}
