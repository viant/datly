package mutationlate

import (
	"context"
	"sync/atomic"
)

var Calls atomic.Int64

type Marker struct{ Id, Name, Code bool }
type Row struct {
	Id   *int64  `sqlx:"id,primaryKey"`
	Name string  `sqlx:"name"`
	Has  *Marker `sqlx:"-" setMarker:"true"`
}
type InsertRow Row

func (*InsertRow) OnInsert(context.Context) error { Calls.Add(1); return nil }

type UpdateRow Row

func (*UpdateRow) OnUpdate(context.Context) error { Calls.Add(1); return nil }

type WrongSignature Row

func (*WrongSignature) OnInsert(context.Context) (string, error) { Calls.Add(1); return "", nil }

type DefaultRow struct {
	Row
	Code string `sqlx:"code,generator=default"`
}
type AutoRow struct {
	Id   *int64  `sqlx:"id,primaryKey,generator=autoincrement"`
	Name string  `sqlx:"name"`
	Has  *Marker `sqlx:"-" setMarker:"true"`
}
