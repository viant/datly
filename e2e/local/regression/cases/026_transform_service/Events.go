package tmp

import (
	"time"
)

type Events struct {
	Id                int                  `sqlx:"name=ID"`
	Quantity          int                  `sqlx:"name=QUANTITY"`
	EventsPerformance []*EventsPerformance `typeName:"EventsPerformance" sqlx:"-"`
}

type EventsPerformance struct {
	Id        int       `sqlx:"name=ID"`
	Price     int       `sqlx:"name=PRICE"`
	EventId   int       `sqlx:"-"`
	Timestamp time.Time `sqlx:"name=TIMESTAMP"`
}

type PerformanceData struct {
	Id    int
	Price int
}
