package jobs

import (
	"context"
	"github.com/viant/xdatly/handler/async"
)

func (s *Service) CreateJob(ctx context.Context, job *async.Job) error {
	db, err := s.connector.DB()
	if err != nil {
		return err
	}
	_, _, err = s.inserter.Exec(ctx, job, db)
	if err != nil {
		return err
	}

	return nil
}
