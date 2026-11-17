package predicate

import (
	"context"
	"fmt"
	"strings"
	"time"

	xexec "github.com/viant/xdatly/exec"
)

// resolveWindow uses the owning invocation's clock without a parallel cache.
// Explicit bounds retain precedence and original parsing/boundary behavior.
func (p *DateRange) resolveWindow(ctx context.Context, in DateRangeInput) (DateRangeInput, error) {
	if in.From != "" || in.To != "" {
		return in, nil
	}
	period := strings.ToLower(strings.TrimSpace(in.Period))
	if period == "" && in.Duration == "" {
		return in, nil
	}
	if period != "" && in.Duration != "" {
		return in, fmt.Errorf("date range requires either period or duration")
	}
	zone := time.UTC
	if p.TimeZone != "" {
		var err error
		zone, err = time.LoadLocation(p.TimeZone)
		if err != nil || p.TimeZone == "Local" {
			return in, fmt.Errorf("date range requires an explicit IANA timezone")
		}
	}
	now := time.Now()
	if execution := xexec.GetContext(ctx); execution != nil && !execution.StartTime.IsZero() {
		now = execution.StartTime
	}
	now = now.In(zone)
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, zone)
	end := start.AddDate(0, 0, 1)
	timestamps := p.Mode == "utc"
	if in.Duration != "" {
		duration, err := time.ParseDuration(in.Duration)
		if err != nil || duration <= 0 {
			return in, fmt.Errorf("date range duration must be positive")
		}
		start, end = now.Add(-duration), now
		_, startOffset := start.Zone()
		_, endOffset := end.Zone()
		if p.Mode != "utc" && startOffset != endOffset {
			return in, fmt.Errorf("elapsed duration across a timezone offset change requires Mode=utc")
		}
		timestamps = true
	} else {
		switch period {
		case "today", "day":
		case "yesterday":
			start, end = start.AddDate(0, 0, -1), start
		case "week", "7d", "seven_days":
			start = start.AddDate(0, 0, -6)
		case "last_complete_7d", "last_complete_7_days":
			start, end = start.AddDate(0, 0, -7), start
		case "month", "30d", "thirty_days":
			start = start.AddDate(0, 0, -29)
		case "month_to_date":
			start = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, zone)
		case "last_month":
			end = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, zone)
			start = end.AddDate(0, -1, 0)
		case "hour":
			start = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, zone)
			end = time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, zone)
			if p.Mode == "utc" {
				// Preserve the current offset in a repeated hour. Constructing the
				// same civil hour with time.Date can select its earlier occurrence.
				start = now.Add(-time.Duration(now.Minute())*time.Minute - time.Duration(now.Second())*time.Second - time.Duration(now.Nanosecond()))
				end = start.Add(time.Hour)
			}
			timestamps = true
		default:
			return in, fmt.Errorf("unsupported date range period %q", period)
		}
	}
	if p.Mode == "utc" {
		in.From, in.To = start.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano)
	} else if timestamps {
		in.From, in.To = start.Format("2006-01-02T15:04:05.999999999"), end.Format("2006-01-02T15:04:05.999999999")
	} else {
		in.From, in.To = start.Format("2006-01-02"), end.AddDate(0, 0, -1).Format("2006-01-02")
	}
	return in, nil
}
