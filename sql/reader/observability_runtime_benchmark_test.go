package reader_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

type adapterLatency struct {
	Mode                                    string
	Run, Workers, N                         int
	NSPerOp, Throughput, P50, P95, P99, Max float64
	Accepted, Dropped, Exported, Failed     uint64
	DrainNS                                 int64
}

func benchmarkSilence() func() {
	old := os.Stdout
	f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	os.Stdout = f
	return func() { os.Stdout = old; f.Close() }
}
func BenchmarkObservedRuntime(b *testing.B) {
	for _, mode := range benchmarkModes {
		for _, parallel := range []bool{false, true} {
			label := "serial"
			if parallel {
				label = "parallel"
			}
			b.Run(mode+"/"+label, func(b *testing.B) {
				restore := benchmarkSilence()
				defer restore()
				f := benchmarkFixture(mode)
				defer f.db.Close()
				if err := f.invoke(context.Background()); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				if parallel {
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							if err := f.invoke(context.Background()); err != nil {
								b.Error(err)
							}
						}
					})
				} else {
					for i := 0; i < b.N; i++ {
						if err := f.invoke(context.Background()); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				d := benchmarkClose(f)
				if d.Accepted+d.Dropped > 0 {
					b.ReportMetric(float64(d.Exported)/float64(d.Accepted+d.Dropped), "export_fraction")
					b.ReportMetric(float64(d.DrainNS)/1e6, "drain_ms")
				}
			})
		}
	}
}
func TestObservedRuntimeLatency(t *testing.T) {
	if os.Getenv("OBS_LATENCY") != "1" {
		t.Skip("set OBS_LATENCY=1")
	}
	restore := benchmarkSilence()
	defer restore()
	output, err := os.Create(os.Getenv("OBS_RESULTS"))
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	encoder := json.NewEncoder(output)
	for run := 0; run < 7; run++ {
		for _, workers := range []int{1, 8} {
			for step := 0; step < len(benchmarkModes); step++ {
				mode := benchmarkModes[(step+run)%len(benchmarkModes)]
				f := benchmarkFixture(mode)
				n := 10000
				values := make([]int64, n)
				var done, ready sync.WaitGroup
				done.Add(workers)
				ready.Add(workers)
				gate := make(chan struct{})
				for worker := 0; worker < workers; worker++ {
					go func(worker int) {
						defer done.Done()
						for j := 0; j < 100; j++ {
							if err := f.invoke(context.Background()); err != nil {
								panic(err)
							}
						}
						ready.Done()
						<-gate
						for i := worker; i < n; i += workers {
							start := time.Now()
							if err := f.invoke(context.Background()); err != nil {
								panic(err)
							}
							values[i] = time.Since(start).Nanoseconds()
						}
					}(worker)
				}
				ready.Wait()
				start := time.Now()
				close(gate)
				done.Wait()
				elapsed := time.Since(start)
				result := benchmarkClose(f)
				f.db.Close()
				raw, err := os.Create(fmt.Sprintf("%s.%s.r%d.w%d.bin", os.Getenv("OBS_RESULTS"), mode, run, workers))
				if err != nil {
					t.Fatal(err)
				}
				if err = binary.Write(raw, binary.LittleEndian, values); err != nil {
					t.Fatal(err)
				}
				raw.Close()
				sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
				result.Mode = mode
				result.Run = run
				result.Workers = workers
				result.N = n
				result.NSPerOp = float64(elapsed.Nanoseconds()) / float64(n)
				result.Throughput = float64(n) / elapsed.Seconds()
				result.P50 = float64(values[n/2])
				result.P95 = float64(values[n*95/100])
				result.P99 = float64(values[n*99/100])
				result.Max = float64(values[n-1])
				if err = encoder.Encode(result); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}
