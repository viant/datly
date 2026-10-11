// Package view preserves the original Datly reader's reusable typed context.
// Reader visitors own these contexts; callers must not share mutable owners
// between concurrent row streams.
package view

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/viant/xdatly/handler"
)

// Context uses the original typed-value map and direct input/DataSync fields.
// Other v1 capabilities retain their existing keys on the parent context.
type Context struct {
	parent   context.Context
	types    map[reflect.Type]interface{}
	input    interface{}
	dataSync *handler.DataSync
	sync.RWMutex
}

func (vc *Context) Deadline() (time.Time, bool) { return vc.parent.Deadline() }
func (vc *Context) Done() <-chan struct{}       { return vc.parent.Done() }
func (vc *Context) Err() error                  { return vc.parent.Err() }

func (vc *Context) Value(key interface{}) interface{} {
	if key == nil {
		return nil
	}
	if t, ok := key.(reflect.Type); ok {
		vc.RLock()
		value, exists := vc.types[t]
		vc.RUnlock()
		if exists {
			return value
		}
	}
	switch key {
	case handler.InputKey:
		return vc.input
	case handler.DataSyncKey:
		if vc.dataSync == nil {
			return nil
		}
		return vc.dataSync
	default:
		return vc.parent.Value(key)
	}
}

func (vc *Context) WithValue(key, value interface{}) context.Context {
	if key == nil {
		return vc
	}
	if t, ok := key.(reflect.Type); ok {
		vc.Lock()
		vc.types[t] = value
		vc.Unlock()
		return vc
	}
	switch key {
	case handler.InputKey:
		vc.input = value
	case handler.DataSyncKey:
		vc.dataSync = value.(*handler.DataSync)
	default:
		vc.parent = context.WithValue(vc.parent, key, value)
	}
	return vc
}

// NewContext always creates a fresh owner, even when parent is reusable.
// Immutable invocation fields are inherited before the context reaches hooks.
func NewContext(parent context.Context) *Context {
	result := &Context{parent: parent, types: make(map[reflect.Type]interface{})}
	result.input = parent.Value(handler.InputKey)
	result.dataSync, _ = parent.Value(handler.DataSyncKey).(*handler.DataSync)
	return result
}

func WithValue(ctx context.Context, key, value interface{}) context.Context {
	if c, ok := ctx.(*Context); ok {
		return c.WithValue(key, value)
	}
	return NewContext(ctx).WithValue(key, value)
}
