package view

import (
	"github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/handler/async"
	"github.com/viant/xdatly/handler/state"
	"golang.org/x/net/context"
	"reflect"
	"sync"
	"time"
)

// Context represents a view context
type Context struct {
	parent         context.Context
	types          map[reflect.Type]interface{}
	input          interface{}
	dbProvider     state.DBProvider
	job            *async.Job
	invocationType async.InvocationType
	dataSync       *handler.DataSync
	sync.RWMutex
}

func (vc *Context) Deadline() (deadline time.Time, ok bool) {
	return vc.parent.Deadline()
}

func (vc *Context) Done() <-chan struct{} {
	return vc.parent.Done()
}

func (vc *Context) Err() error {
	return vc.parent.Err()
}

func (vc *Context) Value(key interface{}) interface{} {
	if key == nil {
		return nil
	}
	if t, ok := key.(reflect.Type); ok {
		vc.RLock()
		ival, exists := vc.types[t]
		vc.RUnlock()
		if exists {
			return ival
		}
	}

	switch key {
	case state.DBProviderKey:
		if vc.dbProvider == nil {
			return nil
		}
		return vc.dbProvider
	case handler.InputKey:
		if vc.input == nil {
			return nil
		}
		return vc.input
	case handler.DataSyncKey:
		if vc.dataSync == nil {
			return nil
		}
		return vc.dataSync
	case async.JobKey:
		if vc.job == nil {
			return nil
		}
		return vc.job
	case async.InvocationTypeKey:
		return vc.invocationType
	default:
		return vc.parent.Value(key)
	}
}

func (vc *Context) WithValue(key interface{}, value interface{}) context.Context {
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
	case state.DBProviderKey:
		vc.dbProvider = value.(state.DBProvider)
	case handler.InputKey:
		vc.input = value
	case handler.DataSyncKey:
		vc.dataSync = value.(*handler.DataSync)
	case async.JobKey:
		vc.job = value.(*async.Job)
	case async.InvocationTypeKey:
		vc.invocationType = value.(async.InvocationType)
	default:
		vc.parent = context.WithValue(vc.parent, key, value)
	}
	return vc
}

func NewContext(parent context.Context) *Context {
	return &Context{
		parent: parent,
		types:  make(map[reflect.Type]interface{}),
	}
}

// WithValue returns a new context with the provided key-value pair.
func WithValue(ctx context.Context, key interface{}, value interface{}) context.Context {
	if c, ok := ctx.(*Context); ok {
		return c.WithValue(key, value)
	}
	c := NewContext(ctx)
	inheritValues(ctx, c)
	return c.WithValue(key, value)
}

func inheritValues(ctx context.Context, c *Context) {
	if ctx != nil {
		if value := ctx.Value(state.DBProviderKey); value != nil {
			if v, ok := value.(state.DBProvider); ok && v != nil {
				c.dbProvider = v
			}
		}
		if value := ctx.Value(async.JobKey); value != nil {
			if v, ok := value.(*async.Job); ok && v != nil {
				c.job = v
			}
		}
		if value := ctx.Value(async.InvocationTypeKey); value != nil {
			if v, ok := value.(async.InvocationType); ok && v != "" {
				c.invocationType = v
			}
		}
		if value := ctx.Value(handler.DataSyncKey); value != nil {
			if v, ok := value.(*handler.DataSync); ok && v != nil {
				c.dataSync = v
			}
		}
		if value := ctx.Value(handler.InputKey); value != nil {
			c.input = value
		}
	}
}
