package exec

import (
	"context"
	"github.com/viant/xdatly/handler/async"
	"sync"
	"time"
)

type contextKey string
type errorKey string

var ContextKey = contextKey("context")
var ErrorKey = errorKey("error")

func GetContext(ctx context.Context) *Context {
	if ctx == nil {
		return nil
	}
	value := ctx.Value(ContextKey)
	if value == nil {
		return nil
	}
	return value.(*Context)
}

// Context represents an execution context
type Context struct {
	mux                        sync.RWMutex
	jobs                       []*async.Job
	values                     map[string]interface{}
	StartTime                  time.Time
	IgnoreEmptyQueryParameters bool
}

func (c *Context) SetValue(key string, value interface{}) {
	c.mux.Lock()
	c.values[key] = value
	c.mux.Unlock()
}

func (c *Context) Value(key string) (interface{}, bool) {
	c.mux.RLock()
	value, has := c.values[key]
	c.mux.RUnlock()
	return value, has
}

func (c *Context) Elapsed() time.Duration {
	now := time.Now()
	return now.Sub(c.StartTime)
}

func (c *Context) EndTime() time.Time {
	now := time.Now()
	return now
}

func (c *Context) AsyncElapsed() time.Duration {
	if len(c.jobs) == 0 {
		return 0
	}
	started := c.jobs[0].CreationTime
	ended := started

	for _, job := range c.jobs {
		if job.CreationTime.Before(started) {
			started = job.CreationTime
		}
		if job.EndTime != nil && job.EndTime.After(ended) {
			ended = *job.EndTime
		}
	}
	if ended == started {
		ended = time.Now()
	}
	return ended.Sub(started)
}

func (c *Context) AsyncEndTime() *time.Time {
	if len(c.jobs) == 0 {
		return nil
	}
	var ret *time.Time
	for _, job := range c.jobs {
		if job.EndTime != nil {
			if ret == nil {
				ret = job.EndTime
			} else if job.EndTime.After(*ret) {
				ret = job.EndTime
			}
		}
	}
	return ret
}

func (c *Context) AsyncCreationTime() *time.Time {
	if len(c.jobs) == 0 {
		return nil
	}
	var ret *time.Time
	for _, job := range c.jobs {
		if ret == nil {
			ret = &job.CreationTime
		} else if job.CreationTime.Before(*ret) {
			ret = &job.CreationTime
		}

	}
	return ret
}

func (c *Context) AppendJob(job *async.Job) {
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.hasJob(job) {
		return
	}

	c.jobs = append(c.jobs, job)
}

func (c *Context) hasJob(job *async.Job) bool {
	for _, candidate := range c.jobs {
		if candidate.ID == job.ID {
			return true
		}
	}
	return false
}

func (c *Context) AsyncStatus() string {
	c.mux.RLock()
	defer c.mux.RUnlock()
	if len(c.jobs) == 0 {
		return "N/A"
	}
	pendingCount := 0
	runningCount := 0
	doneCount := 0
	for _, candidate := range c.jobs {
		if candidate.Status == string(async.StatusDone) || candidate.Status == string(async.StatusError) {
			doneCount++
		} else if candidate.Status == string(async.StatusRunning) {
			runningCount++
		} else if candidate.Status == string(async.StatusPending) {
			pendingCount++
		}
	}
	if doneCount == len(c.jobs) {
		return string(async.StatusDone)
	}
	if pendingCount == len(c.jobs) {
		return string(async.StatusPending)
	}
	return string(async.StatusRunning)
}

func NewContext() *Context {
	return &Context{StartTime: time.Now(), values: map[string]interface{}{}}
}
