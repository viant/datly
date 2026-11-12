package view

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/cache/managed"
	"github.com/viant/datly/logger"
	"github.com/viant/gmetric"
	"github.com/viant/sqlx/io/read"
)

func TestCacheCreationMetricsExposeNativePublications(t *testing.T) {
	ctx := context.Background()
	metrics := gmetric.New()
	counter := metrics.MultiOperationCounter("test", "orders", "cache test", time.Millisecond, time.Minute, 2, newViewMetricProvider())
	v := &View{Name: "orders", Table: "items", Selector: &Config{}, Counter: logger.NewCounter(counter)}
	config := &Cache{Location: t.TempDir(), TimeToLiveMs: 60000}
	factory, err := config.cacheService(v.Name, v)
	require.NoError(t, err)
	service, err := factory()
	require.NoError(t, err)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "data.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec("CREATE TABLE items(id INTEGER);INSERT INTO items VALUES(1)")
	require.NoError(t, err)
	type row struct{ ID int }
	for i := 0; i < 2; i++ {
		r, err := read.New(ctx, db, "SELECT id FROM items", func() any { return &row{} }, read.WithCache(service))
		require.NoError(t, err)
		require.NoError(t, r.QueryAll(ctx, func(any) error { return nil }))
		if stmt := r.Stmt(); stmt != nil {
			require.NoError(t, stmt.Close())
		}
	}
	require.Equal(t, int64(1), metrics.LookupOperationCumulativeMetric("orders", cacheCreatedMetric))
	require.Equal(t, int64(1), metrics.LookupOperationCumulativeMetric("orders", cacheLazyCreatedMetric))
	_, native := service.(*managed.Cache).Cache.(interface{ SetCreationObserver(func(string, int)) })
	if os.Getenv("DATLY_REQUIRE_NATIVE_CREATION_METRICS") == "1" {
		require.True(t, native)
	}
	if !native {
		return
	}
	_, err = service.IndexBy(ctx, db, "id", "SELECT id FROM items", nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), metrics.LookupOperationCumulativeMetric("orders", cacheWarmupCreatedMetric))
	require.Equal(t, int64(3), metrics.LookupOperationCumulativeMetric("orders", cacheCreatedMetric))
	_, err = service.IndexBy(ctx, db, "id", "SELECT id FROM items", nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), metrics.LookupOperationCumulativeMetric("orders", cacheCreatedMetric), "a reused AFS warmup adds no creations")
}
