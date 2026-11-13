package predicate

import (
	"context"
	"fmt"
	"time"

	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	codec "github.com/viant/xdatly/predicate"
)

// DateRangeInput is a bounded reporting range in local calendar or wall-clock time.
// Offset-bearing instants are deliberately not accepted as local wall-clock values.
type DateRangeInput struct {
	From string `json:"from" description:"Inclusive local date (YYYY-MM-DD) or local timestamp without offset."`
	To   string `json:"to" description:"Inclusive local date, or exclusive local timestamp; same precision as from."`
}

// DateRange targets a BigQuery local-time field and its UTC date partition.
// Names are authored configuration, not values supplied by a query caller.
type DateRange struct {
	Alias           string
	DateColumn      string
	TimeColumn      string
	PartitionColumn string
}

func (*DateRange) Description() string {
	return "Filters a bounded local calendar or wall-clock range using typed From/To inputs. Configure DateColumn for YYYY-MM-DD bounds or TimeColumn for offset-free timestamps, optional Alias, and optional UTC DATE PartitionColumn. Dates include both endpoints; timestamps include From and exclude To. Both bounds must use the same precision. When PartitionColumn is configured, its bounds include a one-day margin, valid only when local event time differs from UTC event time by at most 24 hours; it is not appropriate for delayed-ingestion partitions. This implementation uses BigQuery DATE/DATETIME syntax. Pass trusted authored name/value arguments, e.g. DateColumn,local_day,TimeColumn,local_time,PartitionColumn,utc_day,Alias,f. It is an ordinary filter, never authorization."
}

func (p *DateRange) Compute(ctx context.Context, value interface{}, args ...any) (*codec.Criteria, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options, err := codec.Pairs(args...)
	if err != nil {
		return nil, err
	}
	configured := *p
	for name, raw := range options {
		text, ok := raw.(string)
		if !ok || text == "" {
			return nil, fmt.Errorf("date range option %s requires a nonempty string", name)
		}
		switch name {
		case "Alias":
			configured.Alias = text
		case "DateColumn":
			configured.DateColumn = text
		case "TimeColumn":
			configured.TimeColumn = text
		case "PartitionColumn":
			configured.PartitionColumn = text
		default:
			return nil, fmt.Errorf("unknown date range option %q", name)
		}
	}
	p = &configured
	var input DateRangeInput
	switch v := value.(type) {
	case DateRangeInput:
		input = v
	case *DateRangeInput:
		if v == nil {
			return nil, fmt.Errorf("date range requires from and to")
		}
		input = *v
	default:
		return nil, fmt.Errorf("date range requires DateRangeInput, got %T", value)
	}
	parse := func(s string) (time.Time, bool, error) {
		for i, layout := range []string{"2006-01-02", "2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02T15:04", "2006-01-02 15:04"} {
			if t, e := time.Parse(layout, s); e == nil {
				return t, i == 0, nil
			}
		}
		return time.Time{}, false, fmt.Errorf("date range requires a calendar date or offset-free local timestamp")
	}
	from, dates, err := parse(input.From)
	if err != nil {
		return nil, err
	}
	to, toDates, err := parse(input.To)
	if err != nil {
		return nil, err
	}
	if dates != toDates {
		return nil, fmt.Errorf("date range bounds must use the same precision")
	}
	if to.Before(from) || (!dates && to.Equal(from)) {
		return nil, fmt.Errorf("date range end must follow start (equal dates are allowed)")
	}
	alias := p.Alias
	local := p.TimeColumn
	if dates {
		local = p.DateColumn
	}
	if local == "" {
		return nil, fmt.Errorf("date range requires DateColumn for date bounds or TimeColumn for timestamp bounds")
	}
	partition := p.PartitionColumn
	dialect := &info.Dialect{Product: database.Product{Name: "BigQuery"}}
	prefix := ""
	if alias != "" {
		qa, e := dialect.QuoteIdentifier(alias)
		if e != nil {
			return nil, e
		}
		prefix = qa + "."
	}
	ql, err := dialect.QuoteIdentifier(local)
	if err != nil {
		return nil, err
	}
	cast, upper, layout := "DATETIME", "<", "2006-01-02 15:04:05"
	if dates {
		cast, upper, layout = "DATE", "<=", "2006-01-02"
	}
	result := &codec.Criteria{Expression: fmt.Sprintf("%s%s >= %s(?) AND %s%s %s %s(?)", prefix, ql, cast, prefix, ql, upper, cast), Placeholders: []interface{}{from.Format(layout), to.Format(layout)}}
	if partition != "" {
		qp, e := dialect.QuoteIdentifier(partition)
		if e != nil {
			return nil, e
		}
		result.Expression += fmt.Sprintf(" AND %s%s >= DATE(?) AND %s%s <= DATE(?)", prefix, qp, prefix, qp)
		result.Placeholders = append(result.Placeholders, from.AddDate(0, 0, -1).Format("2006-01-02"), to.AddDate(0, 0, 1).Format("2006-01-02"))
	}
	return result, nil
}

var _ codec.VariadicHandler = (*DateRange)(nil)
