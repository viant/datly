package exec

import (
	"github.com/viant/xdatly/handler/async"
	"sync"
	"time"
)

type infoKey string

var InfoKey = infoKey("info")

type Info struct {
	mux       sync.RWMutex
	jobs      []*async.Job
	StartTime time.Time
}

func (i *Info) Elapsed() time.Duration {
	now := time.Now()
	return now.Sub(i.StartTime)
}

func (i *Info) EndTime() time.Time {
	now := time.Now()
	return now
}

func (i *Info) AsyncElapsed() time.Duration {
	if len(i.jobs) == 0 {
		return 0
	}
	started := i.jobs[0].CreationTime
	ended := started

	for _, job := range i.jobs {
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

func (i *Info) AsyncEndTime() *time.Time {
	if len(i.jobs) == 0 {
		return nil
	}
	var ret *time.Time
	for _, job := range i.jobs {
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

func (i *Info) AsyncCreationTime() *time.Time {
	if len(i.jobs) == 0 {
		return nil
	}
	var ret *time.Time
	for _, job := range i.jobs {
		if ret == nil {
			ret = &job.CreationTime
		} else if job.CreationTime.Before(*ret) {
			ret = &job.CreationTime
		}

	}
	return ret
}

func (i *Info) AppendJob(job *async.Job) {
	i.mux.Lock()
	defer i.mux.Unlock()
	if i.hasJob(job) {
		return
	}

	i.jobs = append(i.jobs, job)
}

func (i *Info) hasJob(job *async.Job) bool {
	for _, candidate := range i.jobs {
		if candidate.ID == job.ID {
			return true
		}
	}
	return false
}

func (i *Info) AsyncStatus() string {
	i.mux.RLock()
	defer i.mux.RUnlock()
	if len(i.jobs) == 0 {
		return "N/A"
	}

	pendingCount := 0
	runningCount := 0
	doneCount := 0
	for _, candidate := range i.jobs {
		if candidate.Status == string(async.StatusDone) {
			doneCount++
		} else if candidate.Status == string(async.StatusRunning) {
			runningCount++
		} else if candidate.Status == string(async.StatusPending) {
			pendingCount++
		}
	}
	if doneCount == len(i.jobs) {
		return i.jobs[0].Status
	}
	if pendingCount == len(i.jobs) {
		return string(async.StatusPending)
	}
	return string(async.StatusRunning)
}

func NewInfo() *Info {
	return &Info{StartTime: time.Now()}
}