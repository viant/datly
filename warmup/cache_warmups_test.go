package warmup

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
)

const mixedWarmupsResource = `
CacheProviders:
  - Name: aerospike
    Location: ${view.Name}
    Provider: 'aerospike://127.0.0.1:3000/test'
    TimeToLiveMs: 3600000

Connectors:
  - Name: db
    Driver: sqlite3
    DSN: ":memory:"
  - Name: prewarm
    Driver: sqlite3
    DSN: ":memory:"

Views:
  - Name: performance
    Connector:
      Ref: db
    Table: performance
    Columns:
      - Name: advertiser_id
        DataType: int
      - Name: campaign_id
        DataType: int
    Cache:
      Ref: aerospike
      Warmup:
        IndexColumn: advertiser_id
        Cases:
          - Set:
              - Name: Period
                Values: [today, yesterday]
      Warmups:
        - Name: campaign
          IndexColumn: campaign_id
          Connector:
            Ref: prewarm
          Cases:
            - Set:
                - Name: Period
                  Values: [month]
    Template:
      Source: SELECT * FROM performance WHERE period = $Period
      Parameters:
        - Name: Period
          Required: true
          In:
            Kind: query
            Name: period
          Schema:
            DataType: string
`

func loadWarmupsTestView(t *testing.T, content string) *view.View {
	t.Helper()
	resourcePath := path.Join(t.TempDir(), "resource.yaml")
	require.NoError(t, os.WriteFile(resourcePath, []byte(content), 0644))
	resource, err := view.NewResourceFromURL(context.Background(), resourcePath, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, resource.Views)
	return resource.Views[0]
}

func TestGenerateCacheInputs_MixedSingularAndPluralWarmups(t *testing.T) {
	aView := loadWarmupsTestView(t, mixedWarmupsResource)

	inputs, err := aView.Cache.GenerateCacheInputs(context.Background())
	require.NoError(t, err)
	//two singular advertiser cases followed by one plural campaign case: cases stay
	//scoped to their owning warmup, no cross-warmup cartesian product is generated
	require.Len(t, inputs, 3)

	require.Equal(t, "Period=today", inputs[0].Label)
	require.Equal(t, "advertiser_id", inputs[0].Column)
	require.Same(t, aView.Cache.Warmup, inputs[0].Warmup)

	require.Equal(t, "Period=yesterday", inputs[1].Label)
	require.Equal(t, "advertiser_id", inputs[1].Column)
	require.Same(t, aView.Cache.Warmup, inputs[1].Warmup)

	require.Equal(t, "Period=month", inputs[2].Label)
	require.Equal(t, "campaign_id", inputs[2].Column)
	require.Same(t, aView.Cache.Warmups[0], inputs[2].Warmup)

	//singular compatibility API still generates the singular warmup only
	singularInputs, err := aView.Cache.GenerateCacheInput(context.Background())
	require.NoError(t, err)
	require.Len(t, singularInputs, 2)
	require.Equal(t, "advertiser_id", singularInputs[0].Column)
	require.Equal(t, "advertiser_id", singularInputs[1].Column)
}

func TestWarmupEntryConnectorSelection(t *testing.T) {
	aView := loadWarmupsTestView(t, mixedWarmupsResource)

	inputs, err := aView.Cache.GenerateCacheInputs(context.Background())
	require.NoError(t, err)
	require.Len(t, inputs, 3)

	//singular warmup has no connector: falls back to the view connector
	singularEntry := &warmupEntry{view: aView, warmup: inputs[0].Warmup}
	assert.Equal(t, "db", warmupConnectorLabel(singularEntry))

	//plural warmup carries its own connector selection
	pluralEntry := &warmupEntry{view: aView, warmup: inputs[2].Warmup}
	assert.Equal(t, "prewarm", warmupConnectorLabel(pluralEntry))
}

func TestDBUsesEntryWarmupConnector(t *testing.T) {
	entry := &warmupEntry{
		view: &view.View{
			Connector: view.NewConnector("runtime", "runtime_missing_driver", "runtime_dsn"),
			Cache: &view.Cache{
				Warmup: &view.Warmup{
					Connector: view.NewConnector("singular", "singular_missing_driver", "singular_dsn"),
				},
			},
		},
		warmup: &view.Warmup{
			Connector: view.NewConnector("plural", "plural_missing_driver", "plural_dsn"),
		},
	}

	_, err := DB(entry)

	//the entry uses its originating warmup connector, never the global singular one
	assert.ErrorContains(t, err, "plural_missing_driver")
}

func TestDBFallsBackToViewConnectorForWarmupWithoutConnector(t *testing.T) {
	entry := &warmupEntry{
		view: &view.View{
			Connector: view.NewConnector("runtime", "runtime_missing_driver", "runtime_dsn"),
			Cache: &view.Cache{
				Warmup: &view.Warmup{
					Connector: view.NewConnector("singular", "singular_missing_driver", "singular_dsn"),
				},
			},
		},
		warmup: &view.Warmup{},
	}

	_, err := DB(entry)

	assert.ErrorContains(t, err, "runtime_missing_driver")
}

func TestFilterCacheViews_RecognizesPluralWarmups(t *testing.T) {
	pluralOnly := &view.View{Cache: &view.Cache{Warmups: []*view.Warmup{{IndexColumn: "campaign_id"}}}}
	singularOnly := &view.View{Cache: &view.Cache{Warmup: &view.Warmup{IndexColumn: "advertiser_id"}}}
	noWarmup := &view.View{Cache: &view.Cache{}}
	noCache := &view.View{}

	filtered := FilterCacheViews([]*view.View{pluralOnly, singularOnly, noWarmup, noCache})

	require.Len(t, filtered, 2)
	assert.Same(t, pluralOnly, filtered[0])
	assert.Same(t, singularOnly, filtered[1])
}

const pluralOnlyWarmupsResource = `
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
  - Name: performance
    Connector:
      Ref: db
    Table: performance
    Columns:
      - Name: advertiser_id
        DataType: int
      - Name: campaign_id
        DataType: int
    Cache:
      Ref: aerospike
      Warmups:
        - IndexColumn: advertiser_id
          Cases:
            - Set:
                - Name: Period
                  Values: [today]
        - IndexColumn: campaign_id
          Cases:
            - Set:
                - Name: Period
                  Values: [today, month]
    Template:
      Source: SELECT * FROM performance WHERE period = $Period
      Parameters:
        - Name: Period
          Required: true
          In:
            Kind: query
            Name: period
          Schema:
            DataType: string
`

func TestGenerateCacheInputs_PluralOnlyExecutesEveryWarmup(t *testing.T) {
	aView := loadWarmupsTestView(t, pluralOnlyWarmupsResource)

	inputs, err := aView.Cache.GenerateCacheInputs(context.Background())
	require.NoError(t, err)
	require.Len(t, inputs, 3)
	require.Equal(t, "advertiser_id", inputs[0].Column)
	require.Equal(t, "campaign_id", inputs[1].Column)
	require.Equal(t, "campaign_id", inputs[2].Column)
	require.Same(t, aView.Cache.Warmups[0], inputs[0].Warmup)
	require.Same(t, aView.Cache.Warmups[1], inputs[1].Warmup)

	//without a singular warmup the compatibility API yields nothing
	singularInputs, err := aView.Cache.GenerateCacheInput(context.Background())
	require.NoError(t, err)
	require.Empty(t, singularInputs)
}
