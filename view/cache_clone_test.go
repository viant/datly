package view

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/shared"
	"github.com/viant/sqlx/io/read/cache"
)

func TestCacheCloneForInheritance_StripsRuntimeState(t *testing.T) {
	parent := &View{Name: "parent"}
	limit := 25

	source := &Cache{
		Reference:       shared.Reference{Ref: "cacheRef"},
		Name:            "cacheName",
		Location:        "records/${View.Name}",
		Provider:        "aerospike://127.0.0.1:3000/debrief",
		TimeToLiveMs:    60000,
		PartSize:        1024,
		AerospikeConfig: AerospikeConfig{MaxRetries: 3, TotalTimeoutInMs: 1000},
		owner:           parent,
		_initialized:    true,
		newCache:        func() (cache.Cache, error) { return nil, nil },
		Warmup: &Warmup{
			IndexColumn: "campaign_id",
			Limit:       &limit,
			Connector:   NewConnector("warmup", "sqlite3", "dsn"),
		},
	}

	cloned := source.cloneForInheritance()

	require.NotNil(t, cloned)
	require.NotSame(t, source, cloned)
	require.Nil(t, cloned.owner)
	require.False(t, cloned._initialized)
	require.Nil(t, cloned.newCache)
	require.Equal(t, source.Reference, cloned.Reference)
	require.Equal(t, source.Name, cloned.Name)
	require.Equal(t, source.Location, cloned.Location)
	require.Equal(t, source.Provider, cloned.Provider)
	require.Equal(t, source.TimeToLiveMs, cloned.TimeToLiveMs)
	require.Equal(t, source.PartSize, cloned.PartSize)
	require.Equal(t, source.AerospikeConfig, cloned.AerospikeConfig)
}

func TestCacheCloneForInheritance_DeepCopiesWarmupAndConnectorConfig(t *testing.T) {
	limit := 25
	maxCases := 50
	source := &Cache{
		Warmup: &Warmup{
			IndexColumn:    "campaign_id",
			IndexParameter: "CampaignID",
			IndexMeta:      true,
			Limit:          &limit,
			MaxCases:       &maxCases,
			FieldNames:     []string{"campaign_id", "status"},
			Connector: &Connector{
				Connection: Connection{
					DBConfig: DBConfig{
						Name:   "warmup",
						Driver: "sqlite3",
					},
					_dsn:         "resolved-dsn",
					_initialized: true,
				},
				Connections: []*Connection{
					{
						DBConfig: DBConfig{
							Name:   "replica",
							Driver: "sqlite3",
						},
						_dsn:         "replica-dsn",
						_initialized: true,
					},
				},
				_initialized: true,
			},
			Cases: []*CacheParameters{
				{
					FieldNames: []string{"campaign_id"},
					Set: []*ParamValue{
						{
							Name:           "CampaignID",
							Values:         []interface{}{"A", "B"},
							ExcludeDefault: true,
						},
					},
				},
			},
		},
	}

	cloned := source.cloneForInheritance()

	require.NotNil(t, cloned.Warmup)
	require.NotSame(t, source.Warmup, cloned.Warmup)
	require.NotSame(t, source.Warmup.Connector, cloned.Warmup.Connector)
	require.NotSame(t, source.Warmup.Cases[0], cloned.Warmup.Cases[0])
	require.NotSame(t, source.Warmup.Cases[0].Set[0], cloned.Warmup.Cases[0].Set[0])
	require.NotSame(t, source.Warmup.Connector.Connections[0], cloned.Warmup.Connector.Connections[0])
	require.Equal(t, "resolved-dsn", cloned.Warmup.Connector.getDSN())
	require.Equal(t, "replica-dsn", cloned.Warmup.Connector.Connections[0].getDSN())

	cloned.Warmup.FieldNames[0] = "mutated"
	cloned.Warmup.Cases[0].FieldNames[0] = "mutated_case"
	cloned.Warmup.Cases[0].Set[0].Values[0] = "mutated_value"
	cloned.Warmup.Connector.Connection.DSN = "mutated-dsn"
	cloned.Warmup.Connector.Connections[0].DSN = "mutated-replica-dsn"

	require.Equal(t, "campaign_id", source.Warmup.FieldNames[0])
	require.Equal(t, "campaign_id", source.Warmup.Cases[0].FieldNames[0])
	require.Equal(t, "A", source.Warmup.Cases[0].Set[0].Values[0])
	require.Equal(t, "resolved-dsn", source.Warmup.Connector.getDSN())
	require.Equal(t, "replica-dsn", source.Warmup.Connector.Connections[0].getDSN())
}

func TestViewInherit_ClonesCacheConfiguration(t *testing.T) {
	limit := 25
	parent := &View{
		Cache: &Cache{
			Provider:     "aerospike://127.0.0.1:3000/debrief",
			owner:        &View{Name: "parent"},
			newCache:     func() (cache.Cache, error) { return nil, nil },
			_initialized: true,
			Warmup: &Warmup{
				Limit:      &limit,
				FieldNames: []string{"campaign_id"},
			},
		},
	}
	child := &View{}

	err := child.inherit(parent)

	require.NoError(t, err)
	require.NotNil(t, child.Cache)
	require.NotSame(t, parent.Cache, child.Cache)
	require.NotSame(t, parent.Cache.Warmup, child.Cache.Warmup)
	require.Nil(t, child.Cache.owner)
	require.Nil(t, child.Cache.newCache)
	require.False(t, child.Cache._initialized)

	child.Cache.Warmup.FieldNames[0] = "mutated"
	require.Equal(t, "campaign_id", parent.Cache.Warmup.FieldNames[0])
}
