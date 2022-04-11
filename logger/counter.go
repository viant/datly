package logger

import (
	"github.com/viant/gmetric/counter"
	"time"
)

type Counter interface {
	Begin(started time.Time) counter.OnDone
	DecrementValue(value interface{}) int64
	IncrementValue(value interface{}) int64
}

func NewCounter(counter Counter) *CounterAdapter {
	return &CounterAdapter{
		counter: counter,
	}
}

type CounterAdapter struct {
	counter Counter
}

func (c *CounterAdapter) Begin(started time.Time) counter.OnDone {
	if c.counter == nil {
		return nopOnDone
	}

	return c.counter.Begin(started)
}

func (c *CounterAdapter) DecrementValue(value interface{}) int64 {
	if c.counter == nil {
		return 0
	}

	return c.DecrementValue(value)
}

func (c *CounterAdapter) IncrementValue(value interface{}) int64 {
	if c.counter == nil {
		return 0
	}

	return c.IncrementValue(value)
}

func nopOnDone(_ time.Time, _ ...interface{}) int64 {
	return 0
}
