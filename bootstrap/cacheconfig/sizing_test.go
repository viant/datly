package cacheconfig

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/aerospike"
)

type sizingRow struct {
	ID      int    `sqlx:"id"`
	Payload string `sqlx:"payload"`
}

// BenchmarkConfiguredCacheSizing deliberately requires -benchtime=1x. It measures
// bounded cold-fill and replay work separately, including real AFS object counts.
// Aerospike is opt-in and uses a fixed validation set with 60-second record TTLs.
func BenchmarkConfiguredCacheSizing(b *testing.B) {
	for _, backend := range []string{"afs", "aerospike"} {
		b.Run(backend, func(b *testing.B) {
			uri := backend
			if backend == "aerospike" {
				uri = os.Getenv("DATLY_TEST_AEROSPIKE")
				if uri == "" {
					b.Skip("DATLY_TEST_AEROSPIKE is unset")
				}
			}
			for _, work := range []struct {
				name          string
				rows, entries int
			}{{"rows_1000", 1000, 1}, {"rows_10000", 10000, 1}, {"entries_100", 100, 100}, {"entries_1000", 1000, 1000}} {
				b.Run(work.name, func(b *testing.B) {
					if b.N != 1 {
						b.Fatal("bounded sizing requires -benchtime=1x")
					}
					b.StopTimer()
					ctx := context.Background()
					db := sqlite.New(b)
					if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,payload TEXT)", fmt.Sprintf("WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<%d) INSERT INTO records SELECT x,hex(randomblob(48)) FROM n", work.rows)); err != nil {
						b.Fatal(err)
					}
					location := b.TempDir()
					if backend == "aerospike" {
						location = "datly_validation"
					}
					var pool aerospike.Pool
					b.Cleanup(func() { _ = pool.Close() })
					service, err := (Config{Identity: fmt.Sprintf("sizing/%s/%d", work.name, time.Now().UnixNano()), Aerospike: &pool, Settings: &spec.CacheSettings{Enabled: true, Provider: uri, Location: location, TTL: "60s", TotalTimeoutInMs: 1000, SocketTimeoutInMs: 500}}).New()
					if err != nil {
						b.Fatal(err)
					}
					query := "SELECT id,payload FROM records ORDER BY id"
					if work.entries > 1 {
						query = "SELECT id,payload FROM records WHERE id=?"
					}
					read := func(expectHit bool) {
						for i := 1; i <= work.entries; i++ {
							var args []any
							if work.entries > 1 {
								args = []any{i}
							}
							stats := &cache.Stats{}
							reader, err := sqlxread.New(ctx, db.DB, query, func() any { return &sizingRow{} }, sqlxread.WithCache(service), sqlxread.WithCacheStats(stats))
							if err != nil {
								b.Fatal(err)
							}
							count := 0
							err = reader.QueryAll(ctx, func(value any) error {
								r := value.(*sizingRow)
								if len(r.Payload) != 96 {
									return fmt.Errorf("payload length %d", len(r.Payload))
								}
								count++
								return nil
							}, args...)
							if reader.Stmt() != nil {
								_ = reader.Stmt().Close()
							}
							if err != nil {
								b.Fatal(err)
							}
							want := work.rows / work.entries
							if count != want {
								b.Fatalf("rows=%d want=%d", count, want)
							}
							if expectHit && !stats.FoundAny() {
								b.Fatalf("replay was not a native cache hit: %+v", stats)
							}
						}
					}
					runtime.GC()
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					b.StartTimer()
					start := time.Now()
					read(false)
					fill := time.Since(start)
					b.StopTimer()
					if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
					start = time.Now()
					read(true)
					hit := time.Since(start)
					b.StopTimer()
					runtime.ReadMemStats(&after)
					b.ReportMetric(float64(fill.Nanoseconds())/float64(work.entries), "fill-ns/entry")
					b.ReportMetric(float64(hit.Nanoseconds())/float64(work.entries), "hit-ns/entry")
					b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc), "allocated-B/workload")
					b.ReportMetric(float64(work.rows), "rows/workload")
					b.ReportMetric(float64(work.entries), "entries/workload")
					if after.TotalAlloc-before.TotalAlloc > 256<<20 {
						b.Fatal("256MiB allocation budget exceeded")
					}
					if backend == "afs" {
						var files, bytes, allocated int64
						err := filepath.WalkDir(location, func(path string, d fs.DirEntry, err error) error {
							if err != nil {
								return err
							}
							if d.IsDir() {
								return nil
							}
							info, err := d.Info()
							if err != nil {
								return err
							}
							files++
							bytes += info.Size()
							if stat, ok := info.Sys().(*syscall.Stat_t); ok {
								allocated += stat.Blocks * 512
							}
							return nil
						})
						if err != nil {
							b.Fatal(err)
						}
						b.ReportMetric(float64(files), "files/workload")
						b.ReportMetric(float64(bytes), "logical-B/workload")
						b.ReportMetric(float64(allocated), "disk-B/workload")
						if bytes > 64<<20 || allocated > 64<<20 {
							b.Fatal("64MiB disk budget exceeded")
						}
					}
				})
			}
		})
	}
}
