package dql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWarmupCasesDoNotActivateStorageAndPreserveDeclarationOrder(t *testing.T) {
	const cases = `#setting($_ = $cache_warmup_cases('recent','Period=today','ExcludeDefault=Period'))` + "\n"
	const warmup = `#setting($_ = $cache_warmup('id','CaseRefs=recent','Limit=0'))` + "\n"
	const cache = `#setting($_ = $cache('shared'))` + "\n"
	for _, tc := range []struct {
		name, source  string
		enabled, plan bool
	}{
		{"cases only", cases, false, false},
		{"disabled before cases", "#setting($_ = $cache(false))\n" + cases + warmup, false, true},
		{"disabled after cases", cases + warmup + "#setting($_ = $cache(false))\n", false, true},
		{"cases before storage", cases + cache, true, false},
		{"cases after storage", cache + cases, true, false},
		{"cases then warmup", cases + warmup, true, true},
		{"warmup then cases", warmup + cases, true, true},
		{"cases plan storage", cases + warmup + cache, true, true},
		{"storage plan cases", cache + warmup + cases, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parseComponentSource("example.com/test", "Records", "#setting($_ = $route('/records','GET'))\n"+tc.source+"SELECT * FROM records")
			require.NoError(t, err)
			settings := parsed.Settings.Cache
			require.Equal(t, tc.enabled, settings.Enabled)
			require.Equal(t, tc.plan, settings.HasWarmup())
			require.Len(t, settings.SharedCases["recent"], 1)
			if tc.plan {
				policies, err := settings.EffectiveWarmups()
				require.NoError(t, err)
				require.Len(t, policies, 1)
				require.Equal(t, []string{"today"}, policies[0].Cases[0].Set[0].Values)
			}
		})
	}
}
