// Package castmodel supplies imported rich Go shapes for CAST integration tests.
package castmodel

import "context"

type Bounds struct {
	Unit string     `json:"unit"`
	Cap  int        `json:"cap"`
	Has  *BoundsHas `json:"-" sqlx:"-" setMarker:"true"`
}
type BoundsHas struct{ Unit, Cap bool }
type Record struct {
	ID     int        `json:"id" sqlx:"id,primaryKey=true"`
	Unit   string     `json:"-" sqlx:"unit" internal:"true"`
	Cap    int        `json:"-" sqlx:"cap" internal:"true"`
	Bounds Bounds     `json:"bounds" sqlx:"-"`
	Labels []string   `json:"labels" sqlx:"labels,enc=JSON"`
	Has    *RecordHas `json:"-" sqlx:"-" setMarker:"true"`
}
type RecordHas struct{ ID, Unit, Cap, Bounds, Labels bool }

func (r *Record) OnFetch(context.Context) error {
	r.Bounds = Bounds{Unit: r.Unit, Cap: r.Cap}
	return nil
}

// Init maps only supplied nested fields to their physical backing columns.
func (r *Record) Init(context.Context) error {
	if r.Has == nil {
		r.Has = &RecordHas{}
	}
	if r.Bounds.Has == nil {
		return nil
	}
	if r.Bounds.Has.Unit {
		r.Unit = r.Bounds.Unit
		r.Has.Unit = true
	}
	if r.Bounds.Has.Cap {
		r.Cap = r.Bounds.Cap
		r.Has.Cap = true
	}
	return nil
}
