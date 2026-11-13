package bootstrap

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/aerospike"
	"github.com/viant/sqlx/io/read/cache/afs"
)

type warmupMetadataRow struct {
	ID int `sqlx:"id"`
}
type warmupMetadataSummary struct {
	Total int `sqlx:"total"`
}
type warmupMetadataOutput struct {
	Rows    []warmupMetadataRow
	Summary *warmupMetadataSummary
}

func warmupMetadataArtifact(t *testing.T, settings *spec.CacheSettings, summary *spec.View, bindings ...*spec.ViewBindings) *Artifact {
	t.Helper()
	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/warmup", Name: "Records"},
		Routes:   []*spec.Route{{Method: "GET", Path: "/records"}},
		Settings: &spec.Settings{Cache: settings},
		RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT 1 AS id"}},
	}
	if len(bindings) > 0 {
		component.RootView.Source.Bindings = bindings[0]
	}
	if summary != nil {
		component.RootView.Relations = []*spec.Relation{{Name: "summary", Holder: "Summary", Kind: spec.RelationKindDerived, Cardinality: spec.CardinalityOne, View: summary}}
	}
	artifact, err := BuildArtifact(ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[warmupMetadataOutput](), DirectViewField: "Rows"})
	require.NoError(t, err)
	return artifact
}

func TestWarmupMetadataResolvedBackends(t *testing.T) {
	for _, backend := range []string{"afs", "aerospike"} {
		for _, hostPolicy := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/hostPolicy=%t", backend, hostPolicy), func(t *testing.T) {
				rootStorage := &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}
				var pool aerospike.Pool
				t.Cleanup(func() { require.NoError(t, pool.Close()) })
				if backend == "aerospike" {
					rootStorage.Provider = os.Getenv("DATLY_TEST_AEROSPIKE")
					if rootStorage.Provider == "" {
						t.Skip("DATLY_TEST_AEROSPIKE is unset")
					}
					rootStorage.Location = fmt.Sprintf("datly_meta_%d", time.Now().UnixNano())
				}
				policy := &spec.CacheWarmupSettings{Name: "root-policy", IndexColumn: "id", IndexMeta: true}
				summary := &spec.View{Name: "summary", Source: &spec.ViewSource{SQL: "SELECT COUNT(*) AS total FROM ($View.NonWindowSQL) parent", Bindings: &spec.ViewBindings{CacheName: "summary-cache"}}}
				host := map[string]*spec.CacheSettings{
					"shared":        rootStorage,
					"summary-cache": {Enabled: true, Provider: "AFS", Location: t.TempDir(), TTL: "1m"},
				}
				if hostPolicy {
					summary.Source.Bindings.CacheWarmup = "summary-policy-config"
					host["summary-policy-config"] = &spec.CacheSettings{Warmup: &spec.CacheWarmupSettings{Name: "summary-policy"}}
				}
				artifact := warmupMetadataArtifact(t, &spec.CacheSettings{Enabled: true, Name: "shared", Warmup: policy}, summary)
				execution, err := artifact.ReaderCompilation().NewExecution(ReaderRuntimeConfig{SQL: &dsql.SQLComponent{}, CacheSettings: host, Aerospike: &pool})
				require.NoError(t, err)
				targeter := execution.(dexec.ReaderWarmupTargeter)
				targets := targeter.WarmupTargets()
				wantTargets := 1
				if hostPolicy {
					wantTargets++
				}
				require.Len(t, targets, wantTargets)
				root := targets[0]
				require.Empty(t, root.View)
				require.Equal(t, "shared", root.CacheName)
				require.Equal(t, backend, root.Backend)
				require.Equal(t, "root-policy", root.Settings.Name)
				require.Len(t, root.Work, 2)
				require.Equal(t, "records", root.Work[0].View)
				require.Equal(t, "shared", root.Work[0].CacheName)
				require.Equal(t, backend, root.Work[0].Backend)
				require.Equal(t, "summary", root.Work[1].View)
				require.Equal(t, "summary-cache", root.Work[1].CacheName)
				require.Equal(t, "afs", root.Work[1].Backend)
				if hostPolicy {
					require.Equal(t, "summary-policy", root.Work[1].Settings.Name)
					require.Equal(t, "summary", targets[1].View)
					require.Equal(t, "summary-cache", targets[1].CacheName)
					require.Equal(t, "afs", targets[1].Backend)
				} else {
					require.Equal(t, "root-policy", root.Work[1].Settings.Name)
					require.False(t, root.Work[1].Settings.IndexMeta)
					require.Empty(t, root.Work[1].Settings.IndexColumn)
				}
				encoded, err := json.Marshal(targets)
				require.NoError(t, err)
				require.NotContains(t, string(encoded), rootStorage.Location)
				if rootStorage.Provider != "" {
					require.NotContains(t, string(encoded), rootStorage.Provider)
				}
				require.NotContains(t, string(encoded), host["summary-cache"].Location)
				// Returned policies and work slices cannot change future metadata.
				targets[0].Settings.Name = "mutated"
				targets[0].Work[0].Settings.Name = "mutated work"
				targets[0].Work[1].Backend = "mutated backend"
				again, err := json.Marshal(targeter.WarmupTargets())
				require.NoError(t, err)
				require.JSONEq(t, string(encoded), string(again))
			})
		}
	}
}

// Embedding a native service does not make an opaque supplied service's backend
// knowable: it may route reads elsewhere. No type-name or configured URL guessing.
type opaqueWarmupCache struct{ cache.Cache }

func TestWarmupMetadataSuppliedServices(t *testing.T) {
	for _, name := range []string{"shared", ""} {
		for _, tc := range []struct {
			name    string
			service cache.Cache
			backend string
		}{
			{name: "opaque", service: &opaqueWarmupCache{}},
			{name: "native AFS", service: &afs.Cache{}, backend: "afs"},
			{name: "native Aerospike", service: &aerospike.Cache{}, backend: "aerospike"},
		} {
			t.Run("name="+name+"/"+tc.name, func(t *testing.T) {
				artifact := warmupMetadataArtifact(t, &spec.CacheSettings{Enabled: true, Name: name, Provider: "aerospike://credentials@unreachable.invalid:3000/private", Warmup: &spec.CacheWarmupSettings{Name: "policy"}}, nil)
				execution, err := artifact.ReaderCompilation().NewExecution(ReaderRuntimeConfig{
					SQL: &dsql.SQLComponent{}, ReadCaches: map[string]cache.Cache{"records": tc.service},
					CacheSettings: map[string]*spec.CacheSettings{name: {Enabled: true, Provider: "aerospike://credentials@unreachable.invalid:3000/private", Location: "private-storage", TTL: "1m"}},
				})
				require.NoError(t, err)
				targets := execution.(dexec.ReaderWarmupTargeter).WarmupTargets()
				require.Len(t, targets, 1)
				require.Equal(t, name, targets[0].CacheName)
				require.Equal(t, tc.backend, targets[0].Backend)
				require.Len(t, targets[0].Work, 1)
				require.Equal(t, name, targets[0].Work[0].CacheName)
				require.Equal(t, tc.backend, targets[0].Work[0].Backend)
				encoded, err := json.Marshal(targets)
				require.NoError(t, err)
				require.NotContains(t, string(encoded), "credentials")
				require.NotContains(t, string(encoded), "unreachable.invalid")
				require.NotContains(t, string(encoded), "private-storage")
			})
		}
	}
}

func TestWarmupMetadataCombinedHostPolicyAndStorage(t *testing.T) {
	// With no explicit cache binding, the warmup binding selects both storage
	// and policies from this host configuration.
	artifact := warmupMetadataArtifact(t, nil, nil, &spec.ViewBindings{CacheWarmup: "host-policy-cache"})
	execution, err := artifact.ReaderCompilation().NewExecution(ReaderRuntimeConfig{
		SQL: &dsql.SQLComponent{},
		CacheSettings: map[string]*spec.CacheSettings{"host-policy-cache": {
			Enabled: true, Location: t.TempDir(), TTL: "1m",
			Warmup: &spec.CacheWarmupSettings{Name: "host-policy", IndexColumn: "id"},
		}},
	})
	require.NoError(t, err)
	targets := execution.(dexec.ReaderWarmupTargeter).WarmupTargets()
	require.Len(t, targets, 1)
	require.Equal(t, "host-policy-cache", targets[0].CacheName)
	require.Equal(t, "afs", targets[0].Backend)
	require.Equal(t, "host-policy", targets[0].Settings.Name)
}

func TestWarmupMetadataUnnamedInlineCache(t *testing.T) {
	artifact := warmupMetadataArtifact(t, &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m", Warmup: &spec.CacheWarmupSettings{}}, nil)
	execution, err := artifact.ReaderCompilation().NewExecution(ReaderRuntimeConfig{SQL: &dsql.SQLComponent{}})
	require.NoError(t, err)
	targets := execution.(dexec.ReaderWarmupTargeter).WarmupTargets()
	require.Len(t, targets, 1)
	require.Empty(t, targets[0].CacheName)
	require.Equal(t, "afs", targets[0].Backend)
}
