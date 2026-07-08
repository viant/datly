package warmup

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	datlywarmup "github.com/viant/datly/warmup"
	"github.com/viant/gmetric"
	"github.com/viant/gmetric/stat"
)

func TestAppendPreCachedUsesEntryRows(t *testing.T) {
	response := &Response{}
	result := &datlywarmup.Result{
		Rows: 30,
		Entries: []*datlywarmup.EntryResult{
			{View: "periodSummary#", Column: "order_id", Params: "Period=today", CacheKey: "cache://today", Elapsed: "1s", TimeTaken: time.Second, Rows: 10},
			{View: "periodSummary#", Column: "order_id", Params: "Period=month", CacheKey: "cache://month", FieldNames: "OrderId,Spend", Elapsed: "2s", TimeTaken: 2 * time.Second, Rows: 20},
		},
	}

	appendPreCached(response, "/v1/api/cache/warmup/order", result)

	require.Len(t, response.PreCached, 2)
	require.Equal(t, "Period=today", response.PreCached[0].Params)
	require.Equal(t, "cache://today", response.PreCached[0].CacheKey)
	require.Equal(t, 10, response.PreCached[0].Rows)
	require.Equal(t, "Period=month", response.PreCached[1].Params)
	require.Equal(t, "cache://month", response.PreCached[1].CacheKey)
	require.Equal(t, "OrderId,Spend", response.PreCached[1].FieldNames)
	require.Equal(t, 20, response.PreCached[1].Rows)
	require.Equal(t, "/v1/api/cache/warmup/order", response.PreCached[1].URI)
}

func TestAppendPreCachedPreservesEntryErrors(t *testing.T) {
	response := &Response{}
	result := &datlywarmup.Result{
		Entries: []*datlywarmup.EntryResult{
			{View: "diagnostics", Column: "ad_order_id", Params: "From=2026-07-02", CacheKey: "cache://today", Elapsed: "250ms", TimeTaken: 250 * time.Millisecond, Rows: 7, Error: "failed to index"},
		},
	}

	appendPreCached(response, "/v1/api/cache/warmup/diagnostics", result)

	require.Len(t, response.PreCached, 1)
	require.Equal(t, "failed to index", response.PreCached[0].Error)
}

func TestSummarize(t *testing.T) {
	summary := summarize([]*PreCached{
		{Rows: 10, TimeTaken: 100 * time.Millisecond},
		{Rows: 20, TimeTaken: 200 * time.Millisecond},
		{Rows: 99, TimeTaken: 300 * time.Millisecond, Error: "failed to index"},
		{Rows: 30, TimeTaken: 400 * time.Millisecond},
	})

	require.NotNil(t, summary)
	require.Equal(t, 3, summary.CompletedCases)
	require.Equal(t, 1, summary.FailedCases)
	require.Equal(t, 60, summary.WarmedRows)
}

func TestSummarizeEmpty(t *testing.T) {
	summary := summarize(nil)

	require.NotNil(t, summary)
	require.Zero(t, summary.CompletedCases)
	require.Zero(t, summary.FailedCases)
	require.Zero(t, summary.WarmedRows)
}

func TestSummarizeByView(t *testing.T) {
	summaries := summarizeByView([]*datlywarmup.EntryResult{
		{View: "periodSummary#", Rows: 10, TimeTaken: time.Second},
		{View: "periodSummary#", Rows: 99, TimeTaken: 2 * time.Second, Error: "failed"},
		{View: "timeline#", Rows: 20, TimeTaken: 3 * time.Second},
		{View: "periodSummary#", Rows: 30, TimeTaken: 4 * time.Second},
	})

	require.Len(t, summaries, 2)
	require.Equal(t, "periodSummary#", summaries[0].View)
	require.Equal(t, 2, summaries[0].CompletedCases)
	require.Equal(t, 1, summaries[0].FailedCases)
	require.Equal(t, 40, summaries[0].WarmedRows)
	require.Equal(t, 7*time.Second, summaries[0].Elapsed)
	require.Equal(t, "timeline#", summaries[1].View)
	require.Equal(t, 1, summaries[1].CompletedCases)
	require.Equal(t, 0, summaries[1].FailedCases)
	require.Equal(t, 20, summaries[1].WarmedRows)
	require.Equal(t, 3*time.Second, summaries[1].Elapsed)
}

func TestPreCacheFailureAccounting(t *testing.T) {
	resourcePath := path.Join(t.TempDir(), "resource.yaml")
	require.NoError(t, os.WriteFile(resourcePath, []byte(`
CacheProviders:
  - Name: aerospike
    Location: ${view.Name}
    Provider: 'aerospike://127.0.0.1:3000/test'
    TimeToLiveMs: 3600000

Views:
  - Name: diagnostics
    Connector:
      Ref: db
    Columns:
      - Name: ad_order_id
        DataType: int
    Cache:
      Ref: aerospike
      Warmup:
        IndexColumn: ad_order_id
        Cases:
          - Set:
              - Name: AD_ORDER_ID
                Values: [ abc ]
    Template:
      Source: 'SELECT * FROM DIAGNOSTICS WHERE ad_order_id = $AD_ORDER_ID'
      Parameters:
        - Name: AD_ORDER_ID
          In:
            Kind: query
            Name: ad_order_id
          Schema:
            DataType: int

Connectors:
  - Name: db
    Driver: sqlite3
    DSN: ':memory:'
`), 0644))

	resource, err := view.NewResourceFromURL(context.Background(), resourcePath, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, resource.Views)

	response := PreCache(context.Background(), func(ctx context.Context, method, matchingURI string) ([]*view.View, error) {
		return resource.Views, nil
	}, "/v1/api/cache/warmup/diagnostics")

	require.Equal(t, "error", response.Status)
	require.NotNil(t, response.Summary)
	require.Equal(t, 0, response.Summary.CompletedCases)
	require.Equal(t, 1, response.Summary.FailedCases)
	require.Zero(t, response.Summary.WarmedRows)
	require.Len(t, response.PreCached, 1)
}

func TestPreCacheLookupFailureAccounting(t *testing.T) {
	response := PreCache(context.Background(), func(ctx context.Context, method, matchingURI string) ([]*view.View, error) {
		return nil, fmt.Errorf("lookup failed")
	}, "/v1/api/cache/warmup/diagnostics")

	require.Equal(t, "error", response.Status)
	require.NotNil(t, response.Summary)
	require.Equal(t, 0, response.Summary.CompletedCases)
	require.Equal(t, 1, response.Summary.FailedCases)
	require.Zero(t, response.Summary.WarmedRows)
	require.Len(t, response.PreCached, 1)
	require.Equal(t, "lookup failed", response.PreCached[0].Error)
}

func TestPreCacheAggregatesErrorsDeterministically(t *testing.T) {
	response := PreCache(context.Background(), func(ctx context.Context, method, matchingURI string) ([]*view.View, error) {
		switch matchingURI {
		case "/b":
			return nil, fmt.Errorf("lookup b")
		case "/a":
			return nil, fmt.Errorf("lookup a")
		default:
			return nil, nil
		}
	}, "/b", "/a")

	require.Equal(t, "error", response.Status)
	require.Equal(t, "lookup a; lookup b", response.Error)
	require.NotNil(t, response.Summary)
	require.Equal(t, 0, response.Summary.CompletedCases)
	require.Equal(t, 2, response.Summary.FailedCases)
}

func TestRecordWarmupViewMetrics(t *testing.T) {
	metrics := gmetric.New()
	resource := view.EmptyResource()
	resource.SourceURL = "/tmp/routes/steward/performance/line.yaml"
	resource.Metrics = &view.Metrics{Service: metrics}
	aView := &view.View{Name: "linePeriodSummary#"}
	aView.SetResource(resource)

	recordWarmupViewMetrics(aView, &viewSummary{
		View:           aView.Name,
		CompletedCases: 2,
		FailedCases:    1,
		WarmedRows:     40,
		Elapsed:        1500 * time.Millisecond,
	})

	metricName := warmupMetricName(aView)
	require.Equal(t, int64(1), metrics.LookupOperationCumulativeMetric(metricName, stat.CounterValueKey))
	require.Equal(t, int64(1), metrics.LookupOperationCumulativeMetric(metricName, warmupRunErrorKey))
	require.Equal(t, int64(2), metrics.LookupOperationCumulativeMetric(metricName, warmupCasesCompletedKey))
	require.Equal(t, int64(1), metrics.LookupOperationCumulativeMetric(metricName, warmupCasesFailedKey))
	require.Equal(t, int64(40), metrics.LookupOperationCumulativeMetric(metricName, warmupRowsKey))
	require.GreaterOrEqual(t, metrics.LookupOperationCumulativeMetric(metricName, stat.CounterTimeTakenKey), int64(1500))
}
