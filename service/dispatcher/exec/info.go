package exec

import (
	"fmt"
	"github.com/viant/xdatly/handler/async"
	"sync"
)

type infoKey string

var InfoKey = infoKey("info")

type Info struct {
	mux  sync.Mutex
	jobs []*async.Job
}

func (i *Info) AppendJob(job *async.Job) {
	i.mux.Lock()
	defer i.mux.Unlock()
	if i.hasJob(job) {
		return
	}

	i.jobs = append(i.jobs, job)
	fmt.Printf("added job: %s %v\n", job.ID, len(i.jobs))
}

func (i *Info) hasJob(job *async.Job) bool {
	for _, candidate := range i.jobs {
		if candidate.ID == job.ID {
			return true
		}
	}
	return false
}
