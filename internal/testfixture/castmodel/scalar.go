package castmodel

import "context"

type ScalarValue interface{ int | *int | string }

type Scalar[T ScalarValue] struct {
	ID    int        `json:"id" sqlx:"id,primaryKey=true"`
	Unit  string     `json:"-" sqlx:"unit"`
	Cap   int        `json:"-" sqlx:"cap"`
	Value T          `json:"value" sqlx:"-"`
	Has   *ScalarHas `json:"-" sqlx:"-" setMarker:"true"`
}
type ScalarHas struct{ ID, Unit, Cap, Value bool }

func (r *Scalar[T]) OnFetch(context.Context) error {
	switch any(r.Value).(type) {
	case int:
		r.Value = any(r.Cap).(T)
	case *int:
		v := r.Cap
		r.Value = any(&v).(T)
	case string:
		r.Value = any(r.Unit).(T)
	}
	return nil
}
func (r *Scalar[T]) Init(context.Context) error {
	if r.Has == nil || !r.Has.Value {
		return nil
	}
	switch v := any(r.Value).(type) {
	case int:
		r.Cap = v
		r.Has.Cap = true
	case *int:
		if v != nil {
			r.Cap = *v
			r.Has.Cap = true
		}
	case string:
		r.Unit = v
		r.Has.Unit = true
	}
	return nil
}
