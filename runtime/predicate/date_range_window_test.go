package predicate

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	xexec "github.com/viant/xdatly/exec"
)

func windowClock(t *testing.T, instant string) context.Context {
	t.Helper()
	now, err := time.Parse(time.RFC3339Nano, instant)
	require.NoError(t, err)
	return xexec.WithContext(t.Context(), &xexec.Context{StartTime: now})
}

func TestDateRangePresetCalendarAndElapsedBounds(t *testing.T) {
	for _, tc := range []struct {
		name, now, period, duration, from, to string
	}{
		{"spring day", "2026-03-08T16:00:00Z", "today", "", "2026-03-08T05:00:00Z", "2026-03-09T04:00:00Z"},
		{"fall day", "2026-11-01T17:00:00Z", "today", "", "2026-11-01T04:00:00Z", "2026-11-02T05:00:00Z"},
		{"first repeated hour", "2026-11-01T05:30:00Z", "hour", "", "2026-11-01T05:00:00Z", "2026-11-01T06:00:00Z"},
		{"second repeated hour", "2026-11-01T06:30:00Z", "hour", "", "2026-11-01T06:00:00Z", "2026-11-01T07:00:00Z"},
		{"elapsed through rollback", "2026-11-01T06:30:00Z", "", "1h", "2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z"},
		{"yesterday crosses year", "2027-01-01T17:00:00Z", "yesterday", "", "2026-12-31T05:00:00Z", "2027-01-01T05:00:00Z"},
		{"last complete week", "2026-11-02T17:00:00Z", "last_complete_7d", "", "2026-10-26T04:00:00Z", "2026-11-02T05:00:00Z"},
		{"previous month", "2026-03-17T17:00:00Z", "last_month", "", "2026-02-01T05:00:00Z", "2026-03-01T05:00:00Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &DateRange{Mode: "utc", TimeZone: "America/New_York", TimeColumn: "recorded_at", PartitionColumn: "event_day"}
			ctx := windowClock(t, tc.now)
			input := DateRangeInput{Period: tc.period, Duration: tc.duration}
			first, err := p.Compute(ctx, input)
			require.NoError(t, err)
			require.Equal(t, tc.from, first.Placeholders[0])
			require.Equal(t, tc.to, first.Placeholders[1])
			second, err := p.Compute(ctx, input)
			require.NoError(t, err)
			require.Equal(t, first, second, "one invocation must retain one window")
			require.Empty(t, input.From, "resolution must not mutate caller input")
		})
	}
}

func TestDateRangePresetPrecedenceAndLimits(t *testing.T) {
	ctx := windowClock(t, "2026-11-01T17:00:00Z")
	p := &DateRange{Mode: "utc", TimeZone: "America/New_York", TimeColumn: "observed_at", MaxDuration: "24h"}
	_, err := p.Compute(ctx, DateRangeInput{Period: "today"})
	require.ErrorContains(t, err, "maximum duration", "fall calendar day exceeds 24 elapsed hours")
	_, err = p.Compute(ctx, DateRangeInput{Duration: "24h"})
	require.NoError(t, err)
	explicit := DateRangeInput{From: "2026-11-01T05:00:00Z", To: "2026-11-01T06:00:00Z", Period: "unknown", Duration: "invalid"}
	result, err := p.Compute(ctx, explicit)
	require.NoError(t, err, "explicit bounds retain precedence")
	require.Equal(t, []any{"2026-11-01T05:00:00Z", "2026-11-01T06:00:00Z"}, result.Placeholders)
	for _, input := range []DateRangeInput{
		{Period: "today", Duration: "1h"}, {Period: "invalid"}, {Duration: "0s"}, {Duration: "-1h"}, {Duration: "nonsense"},
		{From: "2026-11-01T05:00:00Z", Period: "today"},
	} {
		_, err = p.Compute(ctx, input)
		require.Error(t, err)
	}
	local := &DateRange{DateColumn: "advertiser_day", TimeZone: "America/New_York"}
	result, err = local.Compute(ctx, DateRangeInput{Period: "last_complete_7d"})
	require.NoError(t, err)
	require.Equal(t, []any{"2026-10-25", "2026-10-31"}, result.Placeholders)
	for _, zone := range []string{"Local", "not/a/zone"} {
		_, err = local.Compute(ctx, DateRangeInput{Period: "today"}, "TimeZone", zone)
		require.ErrorContains(t, err, "IANA timezone")
	}
	_, err = local.Compute(windowClock(t, "2026-11-01T06:30:00Z"), DateRangeInput{Duration: "2h"})
	require.ErrorContains(t, err, "Mode=utc", "local wall-clock fields cannot express an elapsed interval across rollback")
	local.TimeColumn = "advertiser_hour"
	result, err = local.Compute(windowClock(t, "2026-11-01T05:30:00Z"), DateRangeInput{Period: "hour"})
	require.NoError(t, err)
	require.Equal(t, []any{"2026-11-01 01:00:00", "2026-11-01 02:00:00"}, result.Placeholders)
	result, err = local.Compute(windowClock(t, "2026-11-01T17:00:00.25Z"), DateRangeInput{Duration: "1500ms"})
	require.NoError(t, err)
	require.Equal(t, []any{"2026-11-01 11:59:58.75", "2026-11-01 12:00:00.25"}, result.Placeholders)
}
