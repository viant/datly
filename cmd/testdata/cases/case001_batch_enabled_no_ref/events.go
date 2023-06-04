package generated

import (
	"time"
)

type EventTypes struct {
	Id        int            `sqlx:"name=id,primaryKey"`
	Name      string         `sqlx:"name=name"`
	AccountId int            `sqlx:"name=account_id"`
	Events    []*Events      `typeName:"Events" sqlx:"-"`
	Has       *EventTypesHas `setMarker:"true" typeName:"EventTypesHas" json:"-" sqlx:"-" `
}

type Events struct {
	Id          int        `sqlx:"name=id,primaryKey"`
	Timestamp   time.Time  `sqlx:"name=timestamp"`
	EventTypeId int        `sqlx:"name=event_type_id"`
	Quantity    float64    `sqlx:"name=quantity"`
	UserId      int        `sqlx:"name=user_id"`
	Has         *EventsHas `setMarker:"true" typeName:"EventsHas" json:"-" sqlx:"-" `
}

type EventsHas struct {
	Id          bool
	Timestamp   bool
	EventTypeId bool
	Quantity    bool
	UserId      bool
}

type EventTypesHas struct {
	Id        bool
	Name      bool
	AccountId bool
}
