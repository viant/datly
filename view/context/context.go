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
		return vc.dbProvider
	case handler.InputKey:

		return vc.input
	case handler.DataSyncKey:
		return vc.dataSync
	case async.JobKey:
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
		vc.RLock()
		vc.types[t] = value
		vc.RUnlock()
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
	return c.WithValue(key, value)
}
