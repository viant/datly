package outputrows

import "time"

type Row struct {
	ID      int64     `sqlx:"id" json:"id"`
	Metrics []*Metric `view:"metrics,connector=offline" on:"ID:record.id=ID:metrics.id" sql:"SELECT id, stamp, spend FROM metrics" json:"metrics"`
}

type Metric struct {
	ID    int64      `sqlx:"id"`
	Stamp *time.Time `sqlx:"stamp"`
	Spend *float64   `sqlx:"spend"`
}

type Total struct {
	Count int64 `sqlx:"count"`
}
