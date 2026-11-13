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
