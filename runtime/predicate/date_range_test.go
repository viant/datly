package predicate

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDateRangeBoundaries(t *testing.T) {
	p := &DateRange{Alias: "f", DateColumn: "local_day", TimeColumn: "local_time", PartitionColumn: "utc_day"}
	v, e := p.Compute(context.Background(), DateRangeInput{From: "2026-12-31", To: "2027-01-01"})
	require.NoError(t, e)
	require.Contains(t, v.Expression, "`f`.`local_day` <= DATE(?)")
	require.Equal(t, []interface{}{"2026-12-31", "2027-01-01", "2026-12-30", "2027-01-02"}, v.Placeholders)
	v, e = p.Compute(context.Background(), &DateRangeInput{From: "2026-11-01T01:00", To: "2026-11-01T02:00"})
	require.NoError(t, e)
	require.Contains(t, v.Expression, "`f`.`local_time` < DATETIME(?)")
	require.Equal(t, []interface{}{"2026-11-01 01:00:00", "2026-11-01 02:00:00", "2026-10-31", "2026-11-02"}, v.Placeholders)
	for _, x := range []DateRangeInput{{}, {From: "2026-11-02", To: "2026-11-01"}, {From: "2026-11-01", To: "2026-11-01T01:00"}, {From: "2026-11-01T01:00:00-07:00", To: "2026-11-01T02:00:00-07:00"}} {
		_, e = p.Compute(context.Background(), x)
		require.Error(t, e)
	}
	_, e = (&DateRange{Alias: "f` OR TRUE", DateColumn: "local_day"}).Compute(context.Background(), DateRangeInput{From: "2026-11-01", To: "2026-11-01"})
	require.Error(t, e)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e = p.Compute(ctx, DateRangeInput{})
	require.ErrorIs(t, e, context.Canceled)
	require.Contains(t, p.Description(), "never authorization")
}

func TestDateRangeRequiresExplicitColumnsAndSupportsNoPartition(t *testing.T) {
	input := DateRangeInput{From: "2026-11-01", To: "2026-11-01"}
	_, err := (&DateRange{}).Compute(context.Background(), input)
	require.Error(t, err)
	result, err := (&DateRange{}).Compute(context.Background(), input, "DateColumn", "local_day")
	require.NoError(t, err)
	require.Equal(t, "`local_day` >= DATE(?) AND `local_day` <= DATE(?)", result.Expression)
	require.Equal(t, []interface{}{"2026-11-01", "2026-11-01"}, result.Placeholders)
	_, err = (&DateRange{}).Compute(context.Background(), input, "DateColumn", "local_day", "DateColumn", "other")
	require.Error(t, err)
	_, err = (&DateRange{}).Compute(context.Background(), input, "DateColumn", 7)
	require.Error(t, err)
}

func TestDateRangeUTCAndMaximumDuration(t *testing.T) {
	p := &DateRange{Alias: "f", TimeColumn: "recorded_at", PartitionColumn: "event_day"}
	input := DateRangeInput{From: "2026-11-01T01:00:00-07:00", To: "2026-11-01T01:00:00-08:00"}
	result, err := p.Compute(context.Background(), input, "Mode", "utc", "MaxDuration", "24h")
	require.NoError(t, err)
	require.Contains(t, result.Expression, "`f`.`recorded_at` >= TIMESTAMP(?)")
	require.Contains(t, result.Expression, "`f`.`recorded_at` < TIMESTAMP(?)")
	require.Equal(t, []interface{}{"2026-11-01T08:00:00Z", "2026-11-01T09:00:00Z", "2026-11-01", "2026-11-01"}, result.Placeholders)
	require.Empty(t, p.Mode, "authored options must not mutate the shared predicate")
	for _, tc := range []struct {
		from, to string
		allowed  bool
	}{
		{"2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z", true},
		{"2026-11-01T00:00:00.000001Z", "2026-11-02T00:00:00.000002Z", false},
		{"2026-11-01T00:00:00Z", "2026-11-01T00:00:00Z", false},
		{"2026-11-02T00:00:00Z", "2026-11-01T00:00:00Z", false},
		{"2026-11-01", "2026-11-02", false},
		{"2026-11-01T00:00:00", "2026-11-01T01:00:00", false},
	} {
		r, e := p.Compute(context.Background(), DateRangeInput{From: tc.from, To: tc.to}, "Mode", "utc", "MaxDuration", "24h")
		if tc.allowed {
			require.NoError(t, e)
			require.Equal(t, "2026-11-01", r.Placeholders[3])
		} else {
			require.Error(t, e)
			require.Nil(t, r)
		}
	}
	for _, duration := range []string{"0s", "-1h", "nonsense"} {
		_, e := p.Compute(context.Background(), input, "Mode", "utc", "MaxDuration", duration)
		require.Error(t, e)
	}
	_, err = p.Compute(context.Background(), input, "Mode", "unknown")
	require.Error(t, err)
	_, err = p.Compute(context.Background(), input)
	require.Error(t, err, "default local mode must still reject offset timestamps")
	dates := &DateRange{DateColumn: "day", MaxDuration: "24h"}
	_, err = dates.Compute(context.Background(), DateRangeInput{From: "2026-11-01", To: "2026-11-01"})
	require.NoError(t, err)
	_, err = dates.Compute(context.Background(), DateRangeInput{From: "2026-11-01", To: "2026-11-02"})
	require.Error(t, err)
}
