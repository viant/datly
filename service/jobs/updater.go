package jobs

import (
	"context"
	"github.com/viant/xdatly/handler/async"
)

func (s *Service) UpdateJob(ctx context.Context, job *async.Job) error {
	_, err := s.updater.Exec(ctx, job)
	if err != nil {
		return err
	}
	return nil
}
