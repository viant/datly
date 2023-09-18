package exec

import (
	"github.com/viant/xdatly/handler/async"
	"sync"
)

type infoKey string

var InfoKey = infoKey("info")

type Info struct {
	mux  sync.RWMutex
	jobs []*async.Job
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
		return "unknown"
	}
	doneCount := 0
	for _, candidate := range i.jobs {
		if candidate.Status == string(async.StatusDone) {
			doneCount++
		}
	}
	if doneCount == len(i.jobs) {
		return i.jobs[0].Status
	}
	return string(async.StatusRunning)
}
