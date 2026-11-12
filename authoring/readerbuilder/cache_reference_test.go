package readerbuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestServiceEditsNamedCacheWarmup(t *testing.T) {
	service := New(Config{Name: "Records", AvailableCaches: []string{"shared"}})
	cacheSetting := Operation{Type: OperationSetSetting, Setting: &SettingMutation{Name: "cache", Args: []string{"'shared'"}}}
	created := service.Apply(context.Background(), Request{DQL: baseDQL, Operation: cacheSetting})
	require.True(t, created.Applied, "%+v", created.Diagnostics)
	source := created.DQL
	warmup := Operation{Type: OperationSetSetting, Setting: &SettingMutation{
		Name: "cache_warmup", Args: []string{"'id'", "'IndexParameter=IDs'", "'Period=lifetime'", "'ExcludeDefault=Period'", "'Limit=0'"},
	}}
	for _, step := range []string{"add", "reapply", "edit", "omit budgets"} {
		t.Run(step, func(t *testing.T) {
			if step == "edit" {
				warmup.Setting.Args[2] = "'Period=current'"
			}
			if step == "omit budgets" {
				warmup.Setting.Args = warmup.Setting.Args[:4]
			}
			response := service.Apply(context.Background(), Request{DQL: source, Operation: warmup})
			require.True(t, response.Applied, "%+v", response.Diagnostics)
			require.Empty(t, response.Diagnostics)
			settings := response.Structure.Component.Settings.Cache
			require.True(t, settings.Enabled)
			require.Equal(t, "shared", settings.Name)
			require.Empty(t, settings.Provider)
			require.Empty(t, settings.Location)
			require.Empty(t, settings.TTL)
			require.Zero(t, settings.TimeToLiveMs)
			plan := settings.Warmup
			require.NotNil(t, plan)
			require.Equal(t, "id", plan.IndexColumn)
			require.Equal(t, "IDs", plan.IndexParameter)
			if step == "omit budgets" {
				require.Nil(t, plan.Limit)
			} else {
				require.NotNil(t, plan.Limit)
				require.Zero(t, *plan.Limit)
			}
			require.Nil(t, plan.MaxCases)
			require.Len(t, plan.Cases, 1)
			require.Len(t, plan.Cases[0].Set, 1)
			period := plan.Cases[0].Set[0]
			require.Equal(t, "Period", period.Name)
			require.True(t, period.ExcludeDefault)
			want := "lifetime"
			if step == "edit" || step == "omit budgets" {
				want = "current"
			}
			require.Equal(t, []string{want}, period.Values)
			require.Contains(t, response.DQL, "$cache('shared')")
			require.NotContains(t, response.DQL, "WithProvider")
			require.NotContains(t, response.DQL, "WithLocation")
			require.NotContains(t, response.DQL, "WithTimeToLiveMs")
			if step == "reapply" {
				require.Equal(t, source, response.DQL)
			}
			source = response.DQL
		})
	}
	response := service.Apply(context.Background(), Request{DQL: source, Operation: cacheSetting})
	require.True(t, response.Applied, "%+v", response.Diagnostics)
	require.Equal(t, source, response.DQL)
}

func TestServiceRejectsInvalidInlineCacheSettings(t *testing.T) {
	location := "'" + t.TempDir() + "'"
	for _, tc := range []struct {
		name    string
		args    []string
		options []FunctionMutation
	}{
		{name: "TTL only", args: []string{"'shared'", "'1m'"}},
		{name: "location only", args: []string{"'shared'"}, options: []FunctionMutation{{Name: "WithLocation", Args: []string{location}}}},
		{name: "provider only", args: []string{"'shared'"}, options: []FunctionMutation{{Name: "WithProvider", Args: []string{"'afs'"}}}},
		{name: "milliseconds only", args: []string{"'shared'"}, options: []FunctionMutation{{Name: "WithTimeToLiveMs", Args: []string{"60000"}}}},
		{name: "invalid TTL", args: []string{"'shared'", "'bad'"}, options: []FunctionMutation{{Name: "WithLocation", Args: []string{location}}}},
		{name: "unsupported provider", args: []string{"'shared'", "'1m'"}, options: []FunctionMutation{{Name: "WithLocation", Args: []string{location}}, {Name: "WithProvider", Args: []string{"'unknown'"}}}},
		{name: "inline Aerospike", args: []string{"'shared'", "'1m'"}, options: []FunctionMutation{{Name: "WithLocation", Args: []string{location}}, {Name: "WithProvider", Args: []string{"'aerospike://localhost:3000/test'"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := New(Config{Name: "Records", AvailableCaches: []string{"shared"}}).Apply(context.Background(), Request{DQL: baseDQL, Operation: Operation{
				Type: OperationSetSetting, Setting: &SettingMutation{Name: "cache", Args: tc.args, Options: tc.options},
			}})
			require.False(t, response.Applied)
			require.Equal(t, baseDQL, response.DQL)
			require.NotEmpty(t, response.Diagnostics)
		})
	}
}

func TestWarmupEditStillValidatesInlineCache(t *testing.T) {
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{
		DQL:       "#setting($_ = $cache('shared','1m'))\n" + baseDQL,
		Operation: Operation{Type: OperationSetSetting, Setting: &SettingMutation{Name: "cache_warmup", Args: []string{"'id'", "'Limit=0'"}}},
	})
	require.False(t, response.Applied)
	require.NotEmpty(t, response.Diagnostics)
	// Warmup metadata is independent of the native cache storage fields.
	require.Equal(t, &spec.CacheSettings{Enabled: true, Name: "shared", TTL: "1m"}, response.Structure.Component.Settings.Cache)
}
