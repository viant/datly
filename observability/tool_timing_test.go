package observability

import (
	"context"
	"sync"
	"testing"
)

func TestToolTimingIsOptInAndInvocationLocal(t *testing.T) {
	plain := context.Background()
	StartToolTiming(plain, ToolTimingQueryAllInclusive)()
	if got := ToolTimingSnapshot(plain); got != nil {
		t.Fatalf("ordinary request emitted timing: %#v", got)
	}

	first := WithToolTiming(plain)
	second := WithToolTiming(plain)
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			finish := StartToolTiming(first, ToolTimingQueryAllInclusive)
			finish()
			finish() // duplicate completion cannot count twice
		}()
	}
	workers.Wait()
	got := ToolTimingSnapshot(first)
	if len(got) != 1 || got["queryall_inclusive"].Calls != 8 || got["queryall_inclusive"].DurationMs < 0 {
		t.Fatalf("first invocation timing = %#v", got)
	}
	if got := ToolTimingSnapshot(second); got != nil {
		t.Fatalf("independent invocation inherited timing: %#v", got)
	}
	got["queryall_inclusive"] = ToolTimingMetric{Calls: 999}
	if next := ToolTimingSnapshot(first); next["queryall_inclusive"].Calls != 8 {
		t.Fatalf("snapshot mutated collector: %#v", next)
	}
}

func TestToolTimingRejectsUnknownPhaseAndRetainsCancelledObservation(t *testing.T) {
	ctx, cancel := context.WithCancel(WithToolTiming(context.Background()))
	StartToolTiming(ctx, toolTimingPhaseCount)()
	finish := StartToolTiming(ctx, ToolTimingReaderInvoke)
	cancel()
	finish()
	got := ToolTimingSnapshot(ctx)
	if len(got) != 1 || got["reader_invoke"].Calls != 1 {
		t.Fatalf("cancelled invocation timing = %#v", got)
	}
}

func TestToolTimingNumericValuesAreBounded(t *testing.T) {
	ctx := WithToolTiming(context.Background())
	collector := toolTimingFromContext(ctx)
	collector.durations[ToolTimingCatalog] = maxToolTimingDuration
	collector.calls[ToolTimingCatalog] = maxToolTimingCalls
	StartToolTiming(ctx, ToolTimingCatalog)()
	got := ToolTimingSnapshot(ctx)["catalog"]
	if got.Calls != maxToolTimingCalls || got.DurationMs != float64(maxToolTimingDuration.Milliseconds()) {
		t.Fatalf("bounded metric = %#v", got)
	}
}
