package jobs

import (
	"context"
	"github.com/viant/xdatly/handler/async"
)

func (s *Service) CreateJob(ctx context.Context, job *async.Job) error {
	_, _, err := s.inserter.Exec(ctx, job)
	if err != nil {
		return err
	}
	return nil
}
