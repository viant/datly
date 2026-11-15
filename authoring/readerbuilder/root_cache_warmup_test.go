package readerbuilder_test

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	rb "github.com/viant/datly/authoring/readerbuilder"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

const rootCacheWarmupDQL = `#setting($_ = $route('/records','GET'))
SELECT r.*, c.*, use_cache(r,'shared'), cache_warmup(r,'policy'),
use_cache(c,'children'), cache_warmup(c,'policy')
FROM records r LEFT JOIN children c ON c.parent_id=r.ID`

type rootCacheWarmupChild struct {
	ID       int `sqlx:"id"`
	ParentID int `sqlx:"parent_id"`
}
type rootCacheWarmupRow struct {
	ID int                     `sqlx:"ID"`
	C  []*rootCacheWarmupChild `sqlx:"-"`
}
type rootCacheWarmupOutput struct{ Rows []*rootCacheWarmupRow }

func rootCacheWarmupService() *rb.Service {
	return rb.New(rb.Config{Name: "Records", AvailableCaches: []string{"shared", "children"}})
}

func rootCacheWarmupReplacement() rb.Operation {
	return rb.Operation{Type: rb.OperationBatch, Operations: []rb.Operation{
		{Type: rb.OperationRemoveFunction, Function: &rb.FunctionMutation{Name: "cache_warmup", Occurrence: 0, ExpectedArgs: []string{"r", "'policy'"}}},
		{Type: rb.OperationSetSetting, Setting: &rb.SettingMutation{Name: "cache_warmup", Args: []string{"'ID'", "'Limit=0'"}}},
	}}
}

func applyRootCacheWarmup(t *testing.T, source string) *spec.Component {
	t.Helper()
	service := rootCacheWarmupService()
	before := service.Apply(t.Context(), rb.Request{DQL: source, Operation: rb.Operation{Type: rb.OperationInspect}})
	require.Empty(t, before.Diagnostics)
	response := service.Apply(t.Context(), rb.Request{DQL: source, Operation: rootCacheWarmupReplacement()})
	require.True(t, response.Applied, "%+v", response.Diagnostics)
	require.Empty(t, response.Diagnostics)
	require.NotContains(t, response.DQL, "cache_warmup(r,")
	require.Contains(t, response.DQL, "cache_warmup(c,'policy')")
	require.Contains(t, response.DQL, "use_cache(c,'children')")
	if strings.Contains(source, "use_cache(r,") {
		require.Contains(t, response.DQL, "use_cache(r,'shared')")
		require.NotContains(t, response.DQL, "$cache(", "do not add duplicate component storage")
	}
	inspected := service.Apply(t.Context(), rb.Request{DQL: response.DQL, Operation: rb.Operation{Type: rb.OperationInspect}})
	require.Empty(t, inspected.Diagnostics)
	require.Equal(t, response.Structure.Component, inspected.Structure.Component)
	component := inspected.Structure.Component
	require.Equal(t, before.Structure.Component.RootView.Relations[0].View.Source.Bindings, component.RootView.Relations[0].View.Source.Bindings)
	settings := component.Settings.Cache
	require.Empty(t, settings.Provider)
	require.Empty(t, settings.Location)
	require.Empty(t, settings.TTL)
	require.Zero(t, settings.TimeToLiveMs)
	require.Equal(t, "ID", settings.Warmup.IndexColumn)
	require.NotNil(t, settings.Warmup.Limit)
	require.Zero(t, *settings.Warmup.Limit)
	require.Nil(t, settings.Warmup.MaxCases)
	if strings.Contains(source, "use_cache(r,") {
		require.Empty(t, settings.Name)
		require.Equal(t, "shared", component.RootView.Source.Bindings.CacheName)
	} else {
		require.Equal(t, "shared", settings.Name)
	}
	// Reapplying the same warmup setting also uses the root's storage binding.
	reapplied := service.Apply(t.Context(), rb.Request{DQL: response.DQL, Operation: rootCacheWarmupReplacement().Operations[1]})
	require.True(t, reapplied.Applied, "%+v", reapplied.Diagnostics)
	require.Equal(t, response.DQL, reapplied.DQL)
	return component
}

func rootCacheWarmupArtifact(t *testing.T, component *spec.Component) *bootstrap.Artifact {
	t.Helper()
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[rootCacheWarmupOutput](), DirectViewField: "Rows"})
	require.NoError(t, err)
	return artifact
}

func TestRootInlineWarmupUsesOriginalNamedStorage(t *testing.T) {
	for _, storage := range []string{"view", "component"} {
		t.Run(storage, func(t *testing.T) {
			source := rootCacheWarmupDQL
			if storage == "component" {
				source = "#setting($_ = $cache('shared'))\n" + strings.Replace(source, "use_cache(r,'shared'), ", "", 1)
			}
			component := applyRootCacheWarmup(t, source)
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE records(ID INTEGER)", "CREATE TABLE children(id INTEGER,parent_id INTEGER)", "INSERT INTO records VALUES(1)", "INSERT INTO children VALUES(11,1)"))
			rootLocation, childLocation := t.TempDir(), t.TempDir()
			host := map[string]*spec.CacheSettings{
				"shared":   {Enabled: true, Location: rootLocation, TTL: "1m"},
				"children": {Enabled: true, Location: childLocation, TTL: "1m"},
				"policy":   {Warmup: &spec.CacheWarmupSettings{Name: "shared-policy", IndexColumn: "parent_id"}},
			}
			before, err := json.Marshal(host)
			require.NoError(t, err)
			artifact := rootCacheWarmupArtifact(t, component)
			execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}, CacheSettings: host})
			require.NoError(t, err)
			targets := execution.(dexec.ReaderWarmupTargeter).WarmupTargets()
			require.Len(t, targets, 2)
			require.Empty(t, targets[0].View)
			require.Equal(t, "shared", targets[0].CacheName)
			require.Equal(t, "afs", targets[0].Backend)
			require.Equal(t, "ID", targets[0].Settings.IndexColumn)
			require.NotNil(t, targets[0].Settings.Limit)
			require.Zero(t, *targets[0].Settings.Limit)
			require.Equal(t, "c", targets[1].View)
			require.Equal(t, "children", targets[1].CacheName)
			require.Equal(t, "shared-policy", targets[1].Settings.Name)
			require.Equal(t, "parent_id", targets[1].Settings.IndexColumn)
			runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[rootCacheWarmupOutput](), Reader: execution}})
			require.NoError(t, err)
			count, err := runtime.Warmup(t.Context(), dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/records"}})
			require.NoError(t, err)
			require.Equal(t, 2, count)
			for _, location := range []string{rootLocation, childLocation} {
				entries, err := os.ReadDir(location)
				require.NoError(t, err)
				require.NotEmpty(t, entries, "native warmup must populate the original storage")
			}
			after, err := json.Marshal(host)
			require.NoError(t, err)
			require.JSONEq(t, string(before), string(after))
		})
	}
}

func TestRootInlineWarmupRetainsNativeStorageErrors(t *testing.T) {
	component := applyRootCacheWarmup(t, rootCacheWarmupDQL)
	for _, tc := range []struct {
		name     string
		settings *spec.CacheSettings
		want     string
	}{
		{"missing", nil, "location is required"},
		{"disabled", &spec.CacheSettings{Location: t.TempDir(), TTL: "1m"}, "no enabled configuration"},
		{"missing location", &spec.CacheSettings{Enabled: true, TTL: "1m"}, "location is required"},
		{"invalid TTL", &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "bad"}, "TTL must be a positive duration"},
		{"invalid provider", &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m", Provider: "unknown"}, "provider is not supported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			artifact := rootCacheWarmupArtifact(t, component)
			_, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{}, CacheSettings: map[string]*spec.CacheSettings{"shared": tc.settings}})
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestRootInlineWarmupDoesNotBypassStorageValidation(t *testing.T) {
	for _, tc := range []struct{ name, source string }{
		{"no root storage", strings.Replace(rootCacheWarmupDQL, "use_cache(r,'shared'), ", "", 1)},
		{"partial inline TTL", "#setting($_ = $cache('shared','1m'))\n" + rootCacheWarmupDQL},
		{"partial inline provider", "#setting($_ = $cache('shared').WithProvider('afs'))\n" + rootCacheWarmupDQL},
		{"partial inline location", "#setting($_ = $cache('shared').WithLocation('synthetic-cache'))\n" + rootCacheWarmupDQL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := rootCacheWarmupService().Apply(t.Context(), rb.Request{DQL: tc.source, Operation: rootCacheWarmupReplacement()})
			require.False(t, response.Applied)
			require.Equal(t, tc.source, response.DQL)
			require.NotEmpty(t, response.Diagnostics)
		})
	}
}
