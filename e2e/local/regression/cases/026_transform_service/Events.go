package tmp

import (
	"time"
)

type Events struct {
	Id                int                  `sqlx:"ID"`
	Quantity          int                  `sqlx:"QUANTITY"`
	EventsPerformance []*EventsPerformance `typeName:"EventsPerformance" sqlx:"-"`
}

type EventsPerformance struct {
	Id        int       `sqlx:"ID"`
	Price     int       `sqlx:"PRICE"`
	EventId   int       `sqlx:"-"`
	Timestamp time.Time `sqlx:"TIMESTAMP"`
}

type PerformanceData struct {
	Id    int
	Price int
}
