package reader_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

func BenchmarkParityReader(b *testing.B) {
	for _, parallel := range []bool{false, true} {
		label := "serial"
		if parallel {
			label = "parallel"
		}
		b.Run(label, func(b *testing.B) {
			f := newObsRewriteFixture(parityReaderOption())
			defer f.db.Close()
			defer f.app.Shutdown(context.Background())
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
		})
	}
}

func TestParityReaderLatency(t *testing.T) {
	path := os.Getenv("PARITY_LATENCY")
	if path == "" {
		t.Skip("set PARITY_LATENCY")
	}
	output, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	encoder := json.NewEncoder(output)
	for _, workers := range []int{1, 8} {
		f := newObsRewriteFixture(parityReaderOption())
		const n = 5000
		samples := make([]int64, n)
		var ready, done sync.WaitGroup
		ready.Add(workers)
		done.Add(workers)
		gate := make(chan struct{})
		for worker := 0; worker < workers; worker++ {
			go func(worker int) {
				defer done.Done()
				for i := 0; i < 100; i++ {
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
					samples[i] = time.Since(start).Nanoseconds()
				}
			}(worker)
		}
		ready.Wait()
		start := time.Now()
		close(gate)
		done.Wait()
		elapsed := time.Since(start)
		if err := f.app.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		f.db.Close()
		suffix := ".w1.bin"
		if workers == 8 {
			suffix = ".w8.bin"
		}
		raw, err := os.Create(path + suffix)
		if err != nil {
			t.Fatal(err)
		}
		if err := binary.Write(raw, binary.LittleEndian, samples); err != nil {
			t.Fatal(err)
		}
		raw.Close()
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		result := adapterLatency{Workers: workers, N: n, NSPerOp: float64(elapsed.Nanoseconds()) / n, Throughput: float64(n) / elapsed.Seconds(), P50: float64(samples[n/2]), P95: float64(samples[n*95/100]), P99: float64(samples[n*99/100]), Max: float64(samples[n-1])}
		if err := encoder.Encode(result); err != nil {
			t.Fatal(err)
		}
	}
}
