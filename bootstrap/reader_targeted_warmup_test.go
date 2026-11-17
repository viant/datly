package bootstrap

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

func TestTargetedWarmupPresenceAndRootPrecedence(t *testing.T) {
	for _, collection := range []struct {
		name     string
		policies []*spec.CacheWarmupSettings
	}{
		{"nil", nil}, {"empty", []*spec.CacheWarmupSettings{}}, {"all nil", []*spec.CacheWarmupSettings{nil, nil}},
		{"present", []*spec.CacheWarmupSettings{nil, {Name: "targeted", IndexColumn: "id"}}},
	} {
		for _, componentPlan := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/component=%t", collection.name, componentPlan), func(t *testing.T) {
				settings := &spec.CacheSettings{Enabled: true, Name: "shared"}
				if componentPlan {
					settings.Warmup = &spec.CacheWarmupSettings{Name: "component", IndexColumn: "id"}
				}
				artifact := warmupMetadataArtifact(t, settings, nil, &spec.ViewBindings{CacheWarmup: "profile", InlineWarmups: collection.policies})
				execution, err := artifact.ReaderCompilation().NewExecution(ReaderRuntimeConfig{SQL: &dsql.SQLComponent{}, CacheSettings: map[string]*spec.CacheSettings{
					"shared":  {Enabled: true, Location: t.TempDir(), TTL: "1m"},
					"profile": {Warmup: &spec.CacheWarmupSettings{Name: "host", IndexColumn: "id"}},
				}})
				require.NoError(t, err)
				targets := execution.(dexec.ReaderWarmupTargeter).WarmupTargets()
				require.Len(t, targets, 1)
				name, origin, ref := "host", "host", "profile"
				if componentPlan {
					name, origin, ref = "component", "inline", ""
				}
				if collection.name == "present" {
					name, origin, ref = "targeted", "inline", ""
				}
				require.Equal(t, name, targets[0].Settings.Name)
				require.Equal(t, origin, targets[0].Origin)
				require.Equal(t, ref, targets[0].PolicyRef)
			})
		}
	}
}

func TestTargetedWarmupShadowedHostPolicyStillSuppliesStorage(t *testing.T) {
	for _, tc := range []struct {
		name    string
		storage *spec.CacheSettings
		want    string
	}{
		{"valid", &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}, ""},
		{"missing", nil, "no enabled configuration"},
		{"disabled", &spec.CacheSettings{Location: t.TempDir(), TTL: "1m"}, "no enabled configuration"},
		{"invalid location", &spec.CacheSettings{Enabled: true, TTL: "1m"}, "location is required"},
		{"invalid TTL", &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "bad"}, "TTL must be a positive duration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bindings := &spec.ViewBindings{CacheWarmup: "profile", InlineWarmups: []*spec.CacheWarmupSettings{{Name: "targeted", IndexColumn: "id"}}}
			artifact := warmupMetadataArtifact(t, nil, nil, bindings)
			host := map[string]*spec.CacheSettings{"profile": tc.storage}
			if tc.storage != nil {
				tc.storage.Warmup = &spec.CacheWarmupSettings{Name: "ignored-host", CaseRefs: []string{"unresolved-host-cases"}}
			}
			execution, err := artifact.ReaderCompilation().NewExecution(ReaderRuntimeConfig{SQL: &dsql.SQLComponent{}, CacheSettings: host})
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
				return
			}
			require.NoError(t, err)
			target := execution.(dexec.ReaderWarmupTargeter).WarmupTargets()[0]
			require.Equal(t, "targeted", target.Settings.Name)
			require.Equal(t, "profile", target.CacheName)
			require.Equal(t, "inline", target.Origin)
			require.Empty(t, target.PolicyRef)
		})
	}
}

func TestTargetedAndHostWarmupsHaveEquivalentSchedulingAndIndexMetaWork(t *testing.T) {
	zero := 0
	policies := []*spec.CacheWarmupSettings{{Name: "by-id", IndexColumn: "id", IndexMeta: true, Limit: &zero}, {Name: "all", Limit: &zero}}
	var snapshots [][]dexec.ReaderWarmupTarget
	for _, inline := range []bool{false, true} {
		bindings := &spec.ViewBindings{CacheWarmup: "profile"}
		summary := &spec.View{Name: "summary", Source: &spec.ViewSource{SQL: "SELECT COUNT(*) AS total FROM ($View.NonWindowSQL) parent", Bindings: &spec.ViewBindings{CacheName: "summary-cache", CacheWarmup: "summary-profile"}}}
		if inline {
			bindings.InlineWarmups = spec.CloneCacheWarmups(policies)
			summary.Source.Bindings.InlineWarmups = []*spec.CacheWarmupSettings{{Name: "summary"}}
		}
		artifact := warmupMetadataArtifact(t, &spec.CacheSettings{Enabled: true, Name: "shared"}, summary, bindings)
		host := map[string]*spec.CacheSettings{
			"shared":          {Enabled: true, Location: t.TempDir(), TTL: "1m"},
			"summary-cache":   {Enabled: true, Location: t.TempDir(), TTL: "1m"},
			"profile":         {Warmups: spec.CloneCacheWarmups(policies)},
			"summary-profile": {Warmup: &spec.CacheWarmupSettings{Name: "summary"}},
		}
		before, err := json.Marshal(host)
		require.NoError(t, err)
		execution, err := artifact.ReaderCompilation().NewExecution(ReaderRuntimeConfig{SQL: &dsql.SQLComponent{}, CacheSettings: host})
		require.NoError(t, err)
		targets := execution.(dexec.ReaderWarmupTargeter).WarmupTargets()
		require.Len(t, targets, 3)
		require.Len(t, targets[0].Work, 2)
		for i := range targets {
			expectedOrigin := "host"
			if inline {
				expectedOrigin = "inline"
			}
			require.Equal(t, expectedOrigin, targets[i].Origin)
			targets[i].Origin, targets[i].PolicyRef = "", ""
			for j := range targets[i].Work {
				require.Equal(t, expectedOrigin, targets[i].Work[j].Origin)
				targets[i].Work[j].Origin, targets[i].Work[j].PolicyRef = "", ""
			}
		}
		snapshots = append(snapshots, targets)
		after, err := json.Marshal(host)
		require.NoError(t, err)
		require.JSONEq(t, string(before), string(after))
	}
	require.Equal(t, snapshots[0], snapshots[1])
}
