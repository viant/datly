package view

import (
	"reflect"
	"sync"
	"time"

	"github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/handler/async"
	"github.com/viant/xdatly/handler/logger"
	"github.com/viant/xdatly/handler/state"
	"golang.org/x/net/context"
)

// Context represents a view context
type Context struct {
	parent         context.Context
	types          map[reflect.Type]interface{}
	input          interface{}
	logger         logger.Logger
	dbProvider     state.DBProvider
	job            *async.Job
	invocationType async.InvocationType
	dataSync       *handler.DataSync
	sync.RWMutex
}

func (vc *Context) Deadline() (deadline time.Time, ok bool) {
	parent := vc.parentContext()
	return parent.Deadline()
}

func (vc *Context) Done() <-chan struct{} {
	parent := vc.parentContext()
	return parent.Done()
}

func (vc *Context) Err() error {
	parent := vc.parentContext()
	return parent.Err()
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
		vc.RLock()
		dbProvider := vc.dbProvider
		vc.RUnlock()
		if dbProvider == nil {
			return nil
		}
		return dbProvider
	case handler.InputKey:
		vc.RLock()
		input := vc.input
		vc.RUnlock()
		if input == nil {
			return nil
		}
		return input
	case logger.ContextKey:
		vc.RLock()
		aLogger := vc.logger
		vc.RUnlock()
		if aLogger == nil {
			return nil
		}
		return aLogger
	case handler.DataSyncKey:
		vc.RLock()
		dataSync := vc.dataSync
		vc.RUnlock()
		if dataSync == nil {
			return nil
		}
		return dataSync
	case async.JobKey:
		vc.RLock()
		job := vc.job
		vc.RUnlock()
		if job == nil {
			return nil
		}
		return job
	case async.InvocationTypeKey:
		vc.RLock()
		invocationType := vc.invocationType
		vc.RUnlock()
		return invocationType
	default:
		parent := vc.parentContext()
		return parent.Value(key)
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
		vc.Lock()
		defer vc.Unlock()
		vc.dbProvider = value.(state.DBProvider)
	case handler.InputKey:
		vc.Lock()
		defer vc.Unlock()
		vc.input = value
	case logger.ContextKey:
		if value != nil {
			vc.Lock()
			defer vc.Unlock()
			vc.logger = value.(logger.Logger)
		}
	case handler.DataSyncKey:
		vc.Lock()
		defer vc.Unlock()
		vc.dataSync = value.(*handler.DataSync)
	case async.JobKey:
		vc.Lock()
		defer vc.Unlock()
		vc.job = value.(*async.Job)
	case async.InvocationTypeKey:
		vc.Lock()
		defer vc.Unlock()
		vc.invocationType = value.(async.InvocationType)
	default:
		vc.Lock()
		defer vc.Unlock()
		vc.parent = context.WithValue(vc.parent, key, value)
	}
	return vc
}

func (vc *Context) parentContext() context.Context {
	vc.RLock()
	defer vc.RUnlock()
	if vc.parent == nil {
		return context.Background()
	}
	return vc.parent
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
		if value := ctx.Value(logger.ContextKey); value != nil {
			c.logger = value.(logger.Logger)
		}
	}
}
