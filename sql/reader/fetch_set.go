package reader

import (
	"context"
	"sync"

	"github.com/viant/datly/exec"
)

// fetchSet bounds independent read jobs and drains every started worker before
// returning. Callers retain results by ordinal and publish only after success.
type fetchSet struct{ count, concurrency int }

func (s fetchSet) run(ctx context.Context, read func(context.Context, int) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.count == 0 {
		return nil
	}
	concurrency := s.concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > s.count {
		concurrency = s.count
	}
	if concurrency == 1 {
		for index := 0; index < s.count; index++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := runFetch(ctx, index, read); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int, s.count)
	for index := 0; index < s.count; index++ {
		jobs <- index
	}
	close(jobs)
	var workers sync.WaitGroup
	var first error
	var once sync.Once
	workers.Add(concurrency)
	for range concurrency {
		go func() {
			defer workers.Done()
			for index := range jobs {
				if workCtx.Err() != nil {
					continue
				}
				if err := runFetch(workCtx, index, read); err != nil {
					once.Do(func() { first = err; cancel() })
				}
			}
		}()
	}
	workers.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}

// Batch and partition jobs may run outside the relation scheduler's goroutine.
// Recover locally so the owning fetch set can cancel and join every worker.
func runFetch(ctx context.Context, index int, read func(context.Context, int) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = exec.NewPanicError("reader fetch worker", recovered)
		}
	}()
	return read(ctx, index)
}
