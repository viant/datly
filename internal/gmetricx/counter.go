package gmetricx

import (
	"sync"
	"time"

	"github.com/viant/datly/logger"
	"github.com/viant/gmetric"
	"github.com/viant/gmetric/counter"
)

var serviceLocks sync.Map

type OperationRef struct {
	service *gmetric.Service
	name    string
	create  func() *gmetric.Operation
}

type operationCounter struct {
	ref *OperationRef
}

func NewCounter(service *gmetric.Service, name string, create func() *gmetric.Operation) logger.Counter {
	return &operationCounter{
		ref: NewOperationRef(service, name, create),
	}
}

func NewOperationRef(service *gmetric.Service, name string, create func() *gmetric.Operation) *OperationRef {
	return &OperationRef{
		service: service,
		name:    name,
		create:  create,
	}
}

func (c *operationCounter) Begin(started time.Time) counter.OnDone {
	if c == nil || c.ref == nil {
		return func(time.Time, ...interface{}) int64 { return 0 }
	}
	return c.ref.Begin(started)
}

func (c *operationCounter) DecrementValue(value interface{}) int64 {
	if c == nil || c.ref == nil {
		return 0
	}
	return c.ref.DecrementValue(value)
}

func (c *operationCounter) IncrementValue(value interface{}) int64 {
	if c == nil || c.ref == nil {
		return 0
	}
	return c.ref.IncrementValue(value)
}

func (r *OperationRef) Begin(started time.Time) counter.OnDone {
	return func(end time.Time, values ...interface{}) int64 {
		return withOperation(r, func(op *gmetric.Operation) int64 {
			return op.Begin(started)(end, values...)
		})
	}
}

func (r *OperationRef) DecrementValue(value interface{}) int64 {
	return withOperation(r, func(op *gmetric.Operation) int64 {
		return op.DecrementValue(value)
	})
}

func (r *OperationRef) IncrementValue(value interface{}) int64 {
	return withOperation(r, func(op *gmetric.Operation) int64 {
		return op.IncrementValue(value)
	})
}

func (r *OperationRef) IncrementValueBy(value interface{}, delta int64) int64 {
	return withOperation(r, func(op *gmetric.Operation) int64 {
		return op.IncrementValueBy(value, delta)
	})
}

func withOperation(ref *OperationRef, fn func(*gmetric.Operation) int64) int64 {
	if ref == nil || ref.service == nil {
		return 0
	}
	mux := serviceLock(ref.service)
	mux.Lock()
	defer mux.Unlock()

	op := lookupOperationUnlocked(ref.service, ref.name)
	if op == nil && ref.create != nil {
		op = ref.create()
	}
	if op == nil {
		return 0
	}
	return fn(op)
}

func serviceLock(service *gmetric.Service) *sync.Mutex {
	if actual, ok := serviceLocks.Load(service); ok {
		return actual.(*sync.Mutex)
	}
	mux := &sync.Mutex{}
	actual, _ := serviceLocks.LoadOrStore(service, mux)
	return actual.(*sync.Mutex)
}

func lookupOperationUnlocked(service *gmetric.Service, name string) *gmetric.Operation {
	operations := service.OperationCounters()
	for i := range operations {
		if operations[i].Name == name {
			return &operations[i]
		}
	}
	return nil
}
