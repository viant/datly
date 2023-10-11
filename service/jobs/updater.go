package jobs

import (
	"context"
	"github.com/viant/xdatly/handler/async"
)

func (s *Service) UpdateJob(ctx context.Context, job *async.Job) error {
	db, err := s.connector.DB()
	if err != nil {
		return err
	}
	_, err = s.updater.Exec(ctx, job, db)
	if err != nil {
		return err
	}
	return nil
}
