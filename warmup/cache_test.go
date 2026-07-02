package warmup

import (
	"context"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/tests"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/view"
	sqlcache "github.com/viant/sqlx/io/read/cache"
)

func TestPopulateCache(t *testing.T) {
	if os.Getenv("DATLY_RUN_WARMUP_TESTS") == "" {
		t.Skip("set DATLY_RUN_WARMUP_TESTS=1 to run warmup integration test")
	}

	testCases := []struct {
		description      string
		URL              string
		expectedInserted int
		metaIndexed      []interface{}
	}{
		{
			description:      "basic",
			URL:              "case001",
			expectedInserted: 30,
		},
		{
			description:      "template meta",
			URL:              "case002",
			expectedInserted: 64,
			metaIndexed:      []interface{}{2, 11, 111},
		},
		{
			description:      "cache connector",
			URL:              "case003",
			expectedInserted: 64,
			metaIndexed:      []interface{}{2, 11, 111},
		},
		{
			description:      "cache connector",
			URL:              "case004",
			expectedInserted: 8,
		},
		{
			description:      "parent join on",
			URL:              "case005",
			expectedInserted: 2,
		},
	}

	//for _, testCase := range testCases[len(testCases)-1:] {
	for _, testCase := range testCases {
		dataPath := path.Join("testdata", testCase.URL, "populate")
		configPath := path.Join("testdata", "db_config.yaml")

		if !tests.InitDB(t, configPath, dataPath, "db") {
			continue
		}

		resourcePath := path.Join("testdata", testCase.URL, "resource.yaml")

		resource, err := view.NewResourceFromURL(context.TODO(), resourcePath, nil, nil)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		var views []*view.View
		for _, item := range resource.Views {
			views = append(views, item)
		}

		inserted, err := PopulateCache(views)
		assert.Nil(t, err, testCase.description)
		assert.Equal(t, testCase.expectedInserted, inserted, testCase.description)

		for _, aView := range views {
			cache := aView.Cache
			ctx := context.TODO()
			assert.Nil(t, checkIfCached(t, cache, ctx, testCase, aView), testCase.description)
		}
	}
}

func TestWarmupConnectorLabelUsesExplicitWarmupConnector(t *testing.T) {
	aView := &view.View{
		Connector: view.NewRefConnector("bq_metrics"),
		Cache: &view.Cache{
			Warmup: &view.Warmup{
				Connector: view.NewRefConnector("bq_metrics_prewarm"),
			},
		},
	}

	assert.Equal(t, "bq_metrics_prewarm", warmupConnectorLabel(aView))
}

func TestWarmupConnectorLabelFallsBackToViewConnector(t *testing.T) {
	aView := &view.View{
		Connector: view.NewRefConnector("bq_metrics"),
		Cache:     &view.Cache{Warmup: &view.Warmup{}},
	}

	assert.Equal(t, "bq_metrics", warmupConnectorLabel(aView))
}

func TestDBUsesExplicitWarmupConnector(t *testing.T) {
	entry := &warmupEntry{
		view: &view.View{
			Connector: view.NewConnector("runtime", "runtime_missing_driver", "runtime_dsn"),
			Cache: &view.Cache{
				Warmup: &view.Warmup{
					Connector: view.NewConnector("prewarm", "prewarm_missing_driver", "prewarm_dsn"),
				},
			},
		},
	}

	_, err := DB(entry)

	assert.ErrorContains(t, err, "prewarm_missing_driver")
}

func TestWarmupWithLimitCapsConcurrency(t *testing.T) {
	entries := make([]*warmupEntry, 50)
	for i := range entries {
		entries[i] = &warmupEntry{}
	}
	var active int64
	var maxActive int64
	read := func(ctx context.Context, entry *warmupEntry) (*EntryResult, error) {
		current := atomic.AddInt64(&active, 1)
		for {
			max := atomic.LoadInt64(&maxActive)
			if current <= max || atomic.CompareAndSwapInt64(&maxActive, max, current) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		atomic.AddInt64(&active, -1)
		return &EntryResult{Rows: 1}, nil
	}

	notifier := make(chan func() (*EntryResult, error))
	warmupWithLimit(context.Background(), entries, notifier, maxWarmupConcurrency, read)

	total := 0
	for i := 0; i < len(entries); i++ {
		actual := <-notifier
		result, err := actual()
		assert.Nil(t, err)
		total += result.Rows
	}

	assert.Equal(t, len(entries), total)
	assert.LessOrEqual(t, atomic.LoadInt64(&maxActive), int64(maxWarmupConcurrency))
}

func TestWarmupCacheKeyNormalizesNilArgs(t *testing.T) {
	nilArgsKey, err := warmupCacheKey(&sqlcache.ParmetrizedQuery{SQL: "SELECT * FROM events", Args: nil})
	assert.Nil(t, err)

	emptyArgsKey, err := warmupCacheKey(&sqlcache.ParmetrizedQuery{SQL: "SELECT * FROM events", Args: []interface{}{}})
	assert.Nil(t, err)

	assert.Equal(t, emptyArgsKey, nilArgsKey)

	_, err = warmupCacheKey(nil)
	assert.ErrorContains(t, err, "query was nil")
}

func TestWarmupFieldNamesAffectGeneratedCacheKey(t *testing.T) {
	resourcePath := path.Join(t.TempDir(), "resource.yaml")
	require.NoError(t, os.WriteFile(resourcePath, []byte(`
CacheProviders:
  - Name: aerospike
    Location: ${view.Name}
    Provider: 'aerospike://127.0.0.1:3000/test'
    TimeToLiveMs: 3600000

Connectors:
  - Name: db
    Driver: sqlite3
    DSN: ":memory:"

Views:
  - Name: events
    Connector:
      Ref: db
    Table: events
    Columns:
      - Name: event_type_id
        DataType: int
      - Name: quantity
        DataType: int
    Cache:
      Ref: aerospike
      Warmup:
        IndexColumn: event_type_id
    Selector:
      Constraints:
        Projection: true
    Template:
      Source: SELECT * FROM EVENTS
`), 0644))

	resource, err := view.NewResourceFromURL(context.Background(), resourcePath, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, resource.Views)
	aView := resource.Views[0]

	input, err := aView.Cache.GenerateCacheInput(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, input)

	builder := reader.NewBuilder()
	fullQuery, err := builder.CacheSQL(context.Background(), aView, input[0].Selector)
	require.NoError(t, err)
	fullKey, err := warmupCacheKey(fullQuery)
	require.NoError(t, err)

	aView.Cache.Warmup.FieldNames = []string{"Quantity"}
	fieldInput, err := aView.Cache.GenerateCacheInput(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, fieldInput)
	assert.Equal(t, []string{"Quantity"}, fieldInput[0].FieldNames)

	fieldQuery, err := builder.CacheSQL(context.Background(), aView, fieldInput[0].Selector)
	require.NoError(t, err)
	fieldKey, err := warmupCacheKey(fieldQuery)
	require.NoError(t, err)

	assert.NotEqual(t, fullQuery.SQL, fieldQuery.SQL)
	assert.NotEqual(t, fullKey, fieldKey)
	assert.Contains(t, fieldQuery.SQL, "quantity")
}

func TestGenerateCacheInput_AppliesWarmupLimitOverride(t *testing.T) {
	resourcePath := path.Join(t.TempDir(), "resource.yaml")
	require.NoError(t, os.WriteFile(resourcePath, []byte(`
CacheProviders:
  - Name: aerospike
    Location: ${view.Name}
    Provider: 'aerospike://127.0.0.1:3000/test'
    TimeToLiveMs: 3600000

Connectors:
  - Name: db
    Driver: sqlite3
    DSN: ":memory:"

Views:
  - Name: events
    Connector:
      Ref: db
    Table: events
    Columns:
      - Name: event_type_id
        DataType: int
      - Name: quantity
        DataType: int
    Cache:
      Ref: aerospike
      Warmup:
        IndexColumn: event_type_id
        Limit: 25
    Selector:
      Constraints:
        Limit: true
      Limit: 1
    Template:
      Source: SELECT * FROM EVENTS
`), 0644))

	resource, err := view.NewResourceFromURL(context.Background(), resourcePath, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, resource.Views)
	aView := resource.Views[0]

	input, err := aView.Cache.GenerateCacheInput(context.Background())
	require.NoError(t, err)
	require.Len(t, input, 1)
	require.NotNil(t, input[0].Selector)
	assert.Equal(t, 25, input[0].Selector.Limit)
	assert.False(t, input[0].Selector.WarmupNoLimit)
}

func TestGenerateCacheInput_ZeroWarmupLimitSetsNoLimit(t *testing.T) {
	resourcePath := path.Join(t.TempDir(), "resource.yaml")
	require.NoError(t, os.WriteFile(resourcePath, []byte(`
CacheProviders:
  - Name: aerospike
    Location: ${view.Name}
    Provider: 'aerospike://127.0.0.1:3000/test'
    TimeToLiveMs: 3600000

Connectors:
  - Name: db
    Driver: sqlite3
    DSN: ":memory:"

Views:
  - Name: events
    Connector:
      Ref: db
    Table: events
    Columns:
      - Name: event_type_id
        DataType: int
      - Name: quantity
        DataType: int
    Cache:
      Ref: aerospike
      Warmup:
        IndexColumn: event_type_id
        Limit: 0
    Selector:
      Constraints:
        Limit: true
      Limit: 1
    Template:
      Source: SELECT * FROM EVENTS
`), 0644))

	resource, err := view.NewResourceFromURL(context.Background(), resourcePath, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, resource.Views)
	aView := resource.Views[0]

	input, err := aView.Cache.GenerateCacheInput(context.Background())
	require.NoError(t, err)
	require.Len(t, input, 1)
	require.NotNil(t, input[0].Selector)
	assert.Equal(t, 0, input[0].Selector.Limit)
	assert.True(t, input[0].Selector.WarmupNoLimit)
}

func checkIfCached(t *testing.T, cache *view.Cache, ctx context.Context, testCase struct {
	description      string
	URL              string
	expectedInserted int
	metaIndexed      []interface{}
}, aView *view.View) error {
	input, err := cache.GenerateCacheInput(ctx)
	if !assert.Nil(t, err, testCase.description) {
		return err
	}

	service, err := cache.Service()
	if !assert.Nil(t, err, testCase.description) {
		return err
	}

	builder := reader.NewBuilder()

	for _, cacheInput := range input {
		build, err := builder.CacheSQL(ctx, aView, cacheInput.Selector)
		if err != nil {
			return err
		}

		build.By = cacheInput.Column
		entry, err := service.Get(ctx, build.SQL, build.Args, build)
		if err != nil {
			return err
		}

		if assert.True(t, entry.Has(), testCase.description) {
			assert.Nil(t, service.Close(ctx, entry), testCase.description)
		}

		if cacheInput.IndexMeta && aView.Template.Summary != nil {
			metaIndex, err := builder.CacheMetaSQL(ctx, aView, cacheInput.Selector, &view.BatchData{
				ValuesBatch: testCase.metaIndexed,
				Values:      testCase.metaIndexed,
			}, nil, nil)
			if !assert.Nil(t, err, testCase.description) {
				continue
			}

			metaIndex.By = cacheInput.MetaColumn
			metaEntry, err := service.Get(ctx, metaIndex.SQL, metaIndex.Args, metaIndex)
			if !assert.Nil(t, err, testCase.description) {
				continue
			}

			if assert.True(t, metaEntry.Has(), testCase.description) {
				assert.Nil(t, service.Close(ctx, metaEntry), testCase.description)
			}
		}
	}

	return nil
}
