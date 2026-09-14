package reader

import (
	"context"
	"sync"
)

const defaultRelationFetchConcurrency = 4

type relationWork struct {
	run  func(context.Context) error
	skip func()
}

// relationScheduler bounds all recursively spawned relation work for one
// invocation. When no slot is immediately available, the caller executes the
// relation inline; nested scheduling therefore cannot deadlock on the bound.
type relationScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	slots  chan struct{}

	mutex sync.Mutex
	err   error
}

func newRelationScheduler(ctx context.Context, concurrency int) *relationScheduler {
	if concurrency <= 0 {
		concurrency = defaultRelationFetchConcurrency
	}
	workCtx, cancel := context.WithCancel(ctx)
	result := &relationScheduler{ctx: workCtx, cancel: cancel}
	// The invocation caller is one worker. Slots bound additional goroutines.
	if concurrency > 1 {
		result.slots = make(chan struct{}, concurrency-1)
	}
	return result
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
	var started sync.WaitGroup
	for index := range work {
		item := work[index]
		if s.ctx.Err() != nil {
			item.releaseSkipped()
			continue
		}
		if s.tryAcquire() {
			started.Add(1)
			go func() {
				defer started.Done()
				defer s.release()
				s.execute(item)
			}()
			continue
		}
		s.execute(item)
	}
	started.Wait()
	return s.Error()
}

func (s *relationScheduler) execute(work relationWork) {
	if work.run == nil {
		return
	}
	if err := work.run(s.ctx); err != nil {
		s.fail(err)
	}
}

func (s *relationScheduler) tryAcquire() bool {
	if s.slots == nil {
		return false
	}
	select {
	case s.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *relationScheduler) release() {
	if s.slots != nil {
		<-s.slots
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
