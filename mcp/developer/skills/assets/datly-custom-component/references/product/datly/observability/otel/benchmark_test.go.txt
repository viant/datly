package otel

import (
	"context"
	"testing"
	"time"
)

// A fixed burst isolates accepted enqueue/copy work while the exporter is
// blocked. Run with -benchtime=1000x; drain/SDK/export is outside the timer.
func BenchmarkCompletedEnqueue(b *testing.B) {
	if b.N >= 4096 {
		b.Skip("use -benchtime=1000x")
	}
	for _, parallel := range []bool{false, true} {
		name := "serial"
		if parallel {
			name = "parallel"
		}
		b.Run(name, func(b *testing.B) {
			if b.N >= 4096 {
				b.Skip("use -benchtime=1000x")
			}
			gate := make(chan struct{})
			p := &exportProbe{gate: gate, started: make(chan struct{})}
			a, _ := New(Config{Enabled: true, Exporter: p, QueueSize: 4096})
			c := completedFixture()
			a.TrySubmit(c)
			<-p.started
			b.ReportAllocs()
			b.ResetTimer()
			if parallel {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if !a.TrySubmit(c) {
							b.Error("unexpected drop")
						}
					}
				})
			} else {
				for i := 0; i < b.N; i++ {
					if !a.TrySubmit(c) {
						b.Fatal("unexpected drop")
					}
				}
			}
			b.StopTimer()
			close(gate)
			start := time.Now()
			a.Shutdown(context.Background())
			b.ReportMetric(float64(time.Since(start).Nanoseconds())/1e6, "drain_ms")
			if a.Stats().Exported != uint64(b.N+1) {
				b.Fatal("export loss")
			}
		})
	}
}
func BenchmarkCompletedOverflow(b *testing.B) {
	gate := make(chan struct{})
	p := &exportProbe{gate: gate, started: make(chan struct{})}
	a, _ := New(Config{Enabled: true, Exporter: p, QueueSize: 1})
	c := completedFixture()
	a.TrySubmit(c)
	<-p.started
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if a.TrySubmit(c) {
			b.Fatal("overflow accepted")
		}
	}
	b.StopTimer()
	close(gate)
	a.Shutdown(context.Background())
}
func BenchmarkCompletedDisabled(b *testing.B) {
	var a *Adapter
	c := completedFixture()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.TrySubmit(c)
	}
}
