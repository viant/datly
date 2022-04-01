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

func NewCounter() Counter {
	return &nopMetric{}
}

type nopMetric struct{}

func (n *nopMetric) IncrementValue(_ interface{}) int64 {
	return 0
}

func (n *nopMetric) DecrementValue(_ interface{}) int64 {
	return 0
}

func nopOnDone(_ time.Time, _ ...interface{}) int64 {
	return 0
}

func (n *nopMetric) Begin(_ time.Time) counter.OnDone {
	return nopOnDone
}
