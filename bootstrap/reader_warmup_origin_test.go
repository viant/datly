package bootstrap

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

// No DB or warmup call is supplied: provenance must be available at the same
// native initialization boundary as the existing resolved-cache metadata.
func TestWarmupMetadataPolicyOriginAndIndexMeta(t *testing.T) {
	for _, rootOrigin := range []string{"inline", "host"} {
		for _, summaryOrigin := range []string{"inherited", "inline", "host"} {
			t.Run(fmt.Sprintf("root=%s/summary=%s", rootOrigin, summaryOrigin), func(t *testing.T) {
				zero := 0
				rootPolicy := &spec.CacheWarmupSettings{Name: "same-policy-name", IndexColumn: "id", Limit: &zero, IndexMeta: true}
				summaryPolicy := &spec.CacheWarmupSettings{Name: "summary-policy"}
				settings := &spec.CacheSettings{Enabled: true, Name: "shared"}
				rootBindings := &spec.ViewBindings{CacheWarmup: "root-profile"}
				summary := &spec.View{Name: "summary", Source: &spec.ViewSource{SQL: "SELECT COUNT(*) AS total FROM ($View.NonWindowSQL) parent", Bindings: &spec.ViewBindings{CacheName: "summary-cache"}}}
				host := map[string]*spec.CacheSettings{
					"shared":          {Enabled: true, Location: t.TempDir(), TTL: "1m"},
					"summary-cache":   {Enabled: true, Location: t.TempDir(), TTL: "1m"},
					"root-profile":    {Warmup: rootPolicy.Clone(), Provider: "aerospike://credentials@unreachable.invalid/private", Location: "private-profile-storage"},
					"summary-profile": {Warmup: summaryPolicy.Clone()},
				}
				if rootOrigin == "inline" {
					settings.Warmup = rootPolicy.Clone()
				}
				if summaryOrigin == "host" {
					summary.Source.Bindings.CacheWarmup = "summary-profile"
				}
				artifact := warmupMetadataArtifact(t, settings, summary, rootBindings)
				if summaryOrigin == "inline" {
					// Exercise an existing directly supplied runtime-view policy. This
					// does not introduce new inline-child authoring syntax.
					artifact.Reader.Root.Relations[0].Target.View.Cache = &data.Cache{Name: "summary-cache", Warmup: summaryPolicy.Clone()}
				}
				beforeHost, err := json.Marshal(host)
				require.NoError(t, err)
				execution, err := artifact.ReaderCompilation().NewExecution(ReaderRuntimeConfig{SQL: &dsql.SQLComponent{}, CacheSettings: host})
				require.NoError(t, err)
				targeter := execution.(dexec.ReaderWarmupTargeter)
				targets := targeter.WarmupTargets()
				wantTargets := 2
				if summaryOrigin == "inherited" {
					wantTargets = 1
				}
				require.Len(t, targets, wantTargets, "descriptive work must not add scheduled targets")
				rootRef := ""
				if rootOrigin == "host" {
					rootRef = "root-profile"
				}
				require.Equal(t, rootOrigin, targets[0].Origin)
				require.Equal(t, rootRef, targets[0].PolicyRef)
				require.Equal(t, "same-policy-name", targets[0].Settings.Name)
				require.Equal(t, "shared", targets[0].CacheName)
				require.Equal(t, "afs", targets[0].Backend)
				require.NotNil(t, targets[0].Settings.Limit)
				require.Zero(t, *targets[0].Settings.Limit)
				require.Nil(t, targets[0].Settings.MaxCases)
				require.Len(t, targets[0].Work, 2)
				require.Equal(t, rootOrigin, targets[0].Work[0].Origin)
				require.Equal(t, rootRef, targets[0].Work[0].PolicyRef)
				workOrigin, workRef := summaryOrigin, ""
				if summaryOrigin == "inherited" {
					workOrigin, workRef = rootOrigin, rootRef
				}
				if summaryOrigin == "host" {
					workRef = "summary-profile"
				}
				require.Equal(t, workOrigin, targets[0].Work[1].Origin)
				require.Equal(t, workRef, targets[0].Work[1].PolicyRef)
				require.Equal(t, "summary-cache", targets[0].Work[1].CacheName)
				if wantTargets == 2 {
					require.Equal(t, workOrigin, targets[1].Origin)
					require.Equal(t, workRef, targets[1].PolicyRef)
					require.Equal(t, workOrigin, targets[1].Work[0].Origin)
					require.Equal(t, workRef, targets[1].Work[0].PolicyRef)
				}
				encoded, err := json.Marshal(targets)
				require.NoError(t, err)
				for _, secret := range []string{host["shared"].Location, host["summary-cache"].Location, "credentials", "unreachable.invalid", "private-profile-storage"} {
					require.NotContains(t, string(encoded), secret)
				}
				targets[0].Origin, targets[0].PolicyRef = "mutated", "mutated"
				targets[0].Work[1].Origin, targets[0].Work[1].PolicyRef = "mutated", "mutated"
				again, err := json.Marshal(targeter.WarmupTargets())
				require.NoError(t, err)
				require.JSONEq(t, string(encoded), string(again))
				afterHost, err := json.Marshal(host)
				require.NoError(t, err)
				require.JSONEq(t, string(beforeHost), string(afterHost))
			})
		}
	}
}

func TestWarmupMetadataSharedHostProfileAndPluralPolicies(t *testing.T) {
	profile := &spec.CacheSettings{
		Warmup:  &spec.CacheWarmupSettings{Name: "by-id", IndexColumn: "id", IndexMeta: true},
		Warmups: []*spec.CacheWarmupSettings{{Name: "by-other", IndexColumn: "other"}},
	}
	summary := &spec.View{Name: "summary", Source: &spec.ViewSource{SQL: "SELECT COUNT(*) AS total FROM ($View.NonWindowSQL) parent", Bindings: &spec.ViewBindings{CacheName: "summary-cache", CacheWarmup: "profile"}}}
	artifact := warmupMetadataArtifact(t, &spec.CacheSettings{Enabled: true, Name: "shared"}, summary, &spec.ViewBindings{CacheWarmup: "profile"})
	execution, err := artifact.ReaderCompilation().NewExecution(ReaderRuntimeConfig{SQL: &dsql.SQLComponent{}, CacheSettings: map[string]*spec.CacheSettings{
		"shared":        {Enabled: true, Location: t.TempDir(), TTL: "1m"},
		"summary-cache": {Enabled: true, Location: t.TempDir(), TTL: "1m"},
		"profile":       profile,
	}})
	require.NoError(t, err)
	targets := execution.(dexec.ReaderWarmupTargeter).WarmupTargets()
	require.Len(t, targets, 4, "two policies on each of two views")
	for i, target := range targets {
		require.Equal(t, "host", target.Origin)
		require.Equal(t, "profile", target.PolicyRef)
		wantName := "by-id"
		if i%2 == 1 {
			wantName = "by-other"
		}
		require.Equal(t, wantName, target.Settings.Name)
		for _, work := range target.Work {
			require.Equal(t, "host", work.Origin)
			require.Equal(t, "profile", work.PolicyRef)
		}
	}
	require.Len(t, targets[0].Work, 3)
	require.Equal(t, "by-id", targets[0].Work[1].Settings.Name)
	require.Equal(t, "by-other", targets[0].Work[2].Settings.Name)
}

func TestWarmupMetadataSelectedAndShadowedHostReferences(t *testing.T) {
	for _, inline := range []bool{false, true} {
		for _, emptyProfile := range []bool{false, true} {
			t.Run(fmt.Sprintf("inline=%t/emptyProfile=%t", inline, emptyProfile), func(t *testing.T) {
				settings := &spec.CacheSettings{Enabled: true, Name: "shared"}
				if inline {
					settings.Warmup = &spec.CacheWarmupSettings{Name: "inline-policy", IndexColumn: "id"}
				}
				artifact := warmupMetadataArtifact(t, settings, nil, &spec.ViewBindings{CacheWarmup: "unresolved"})
				host := map[string]*spec.CacheSettings{"shared": {Enabled: true, Location: t.TempDir(), TTL: "1m"}}
				if emptyProfile {
					host["unresolved"] = &spec.CacheSettings{}
				}
				execution, err := artifact.ReaderCompilation().NewExecution(ReaderRuntimeConfig{SQL: &dsql.SQLComponent{}, CacheSettings: host})
				if !inline {
					require.ErrorContains(t, err, `cache warmup "unresolved" has no configuration`)
					require.Nil(t, execution)
					return
				}
				require.NoError(t, err)
				targets := execution.(dexec.ReaderWarmupTargeter).WarmupTargets()
				require.Len(t, targets, 1)
				require.Equal(t, "inline", targets[0].Origin)
				require.Empty(t, targets[0].PolicyRef)
				require.Equal(t, "inline", targets[0].Work[0].Origin)
				require.Empty(t, targets[0].Work[0].PolicyRef)
			})
		}
	}
}
