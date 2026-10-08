package reader

import (
	"context"
	"sync"

	"github.com/viant/datly/exec"
)

const defaultRelationFetchConcurrency = 4

type relationWork struct {
	run      func(context.Context) error
	children func() []relationWork
	complete func(context.Context) error
	// finish releases started work after descendants finish (also on failure).
	// skip releases work that never started; these callbacks are exclusive.
	finish func(error)
	skip   func()
}

// relationScheduler bounds runnable phases, not entire relation lifetimes.
// Parents waiting for descendants consume no worker; only the coordinator
// owns dependency counts and publishes their completion phases.
type relationScheduler struct {
	ctx     context.Context
	cancel  context.CancelFunc
	workers int

	mutex sync.Mutex
	err   error
}

func newRelationScheduler(ctx context.Context, concurrency int) *relationScheduler {
	if concurrency <= 0 {
		concurrency = defaultRelationFetchConcurrency
	}
	workCtx, cancel := context.WithCancel(ctx)
	return &relationScheduler{ctx: workCtx, cancel: cancel, workers: concurrency}
}

func (s *relationScheduler) Close() {
	if s != nil && s.cancel != nil {
		s.cancel()
	}
}

func (s *relationScheduler) Run(work []relationWork) error {
	if s == nil {
		return nil
	}
	if len(work) == 0 {
		return s.Error()
	}
	jobs := make(chan *relationTask)
	results := make(chan relationResult, s.workers)
	var workers sync.WaitGroup
	for i := 0; i < s.workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for task := range jobs {
				results <- s.execute(task)
			}
		}()
	}
	defer func() {
		close(jobs)
		workers.Wait()
	}()
	queue := make([]*relationTask, 0, len(work))
	for _, item := range work {
		queue = append(queue, &relationTask{work: item})
	}
	remaining := len(queue)
	for remaining > 0 {
		var next *relationTask
		var ready chan *relationTask
		if len(queue) > 0 {
			next, ready = queue[0], jobs
		}
		select {
		case ready <- next:
			queue[0] = nil
			queue = queue[1:]
		case result := <-results:
			task := result.task
			if len(result.children) > 0 {
				task.pending = len(result.children)
				remaining += task.pending
				for _, child := range result.children {
					queue = append(queue, &relationTask{work: child, parent: task})
				}
				continue
			}
			remaining--
			if parent := task.parent; parent != nil {
				parent.pending--
				if parent.pending == 0 {
					queue = append(queue, parent)
				}
			}
		}
	}
	return s.Error()
}

type relationTask struct {
	work     relationWork
	parent   *relationTask
	pending  int
	prepared bool
}

type relationResult struct {
	task     *relationTask
	children []relationWork
}

func (s *relationScheduler) execute(task *relationTask) relationResult {
	result := relationResult{task: task}
	work := task.work
	if !task.prepared {
		if s.ctx.Err() != nil {
			s.protect(func() error { work.releaseSkipped(); return nil })
			return result
		}
		task.prepared = true
		s.protect(func() error {
			if work.run != nil {
				if err := work.run(s.ctx); err != nil {
					return err
				}
			}
			if s.ctx.Err() == nil && work.children != nil {
				result.children = work.children()
			}
			return nil
		})
		if len(result.children) > 0 {
			return result
		}
	}
	if s.ctx.Err() == nil && work.complete != nil {
		s.protect(func() error { return work.complete(s.ctx) })
	}
	// Cleanup belongs to started work even when preparation/completion panics.
	if work.finish != nil {
		s.protect(func() error { work.finish(s.Error()); return nil })
	}
	return result
}

func (s *relationScheduler) protect(run func() error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.fail(exec.NewPanicError("relation worker", recovered))
		}
	}()
	if err := run(); err != nil {
		s.fail(err)
	}
}

func (s *relationScheduler) fail(err error) {
	if err == nil {
		return
	}
	s.mutex.Lock()
	if s.err == nil {
		s.err = err
		s.cancel()
	}
	s.mutex.Unlock()
}

func (s *relationScheduler) Error() error {
	if s == nil {
		return nil
	}
	s.mutex.Lock()
	err := s.err
	s.mutex.Unlock()
	if err != nil {
		return err
	}
	return s.ctx.Err()
}

func (w relationWork) releaseSkipped() {
	if w.skip != nil {
		w.skip()
	}
}
