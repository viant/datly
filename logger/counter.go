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

func (c *CounterAdapter) Counter() Counter {
	return c.counter
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
	return c.counter.DecrementValue(value)
}

func (c *CounterAdapter) IncrementValue(value interface{}) int64 {
	if c.counter == nil {
		return 0
	}
	return c.counter.IncrementValue(value)
}

func nopOnDone(_ time.Time, _ ...interface{}) int64 {
	return 0
}

// IncrementValueBy updates a counter in one operation when supported.
func IncrementValueBy(c Counter, value interface{}, delta int64) int64 {
	if c == nil {
		return 0
	}
	if bulk, ok := c.(interface {
		IncrementValueBy(interface{}, int64) int64
	}); ok {
		return bulk.IncrementValueBy(value, delta)
	}
	var result int64
	for ; delta > 0; delta-- {
		result = c.IncrementValue(value)
	}
	for ; delta < 0; delta++ {
		result = c.DecrementValue(value)
	}
	return result
}
func (c *CounterAdapter) IncrementValueBy(value interface{}, delta int64) int64 {
	return IncrementValueBy(c.counter, value, delta)
}
