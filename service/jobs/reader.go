package jobs

import (
	"context"
	"github.com/viant/datly/service/reader"
	"github.com/viant/xdatly/handler/async"
)

const (
	viewID = "datly_jobs"
)

func (s *Service) JobById(ctx context.Context, jobId string) (*async.Job, error) {
	var result = []*async.Job{}
	session, err := reader.NewSession(&result, s.readerView, reader.WithDryRun())
	if err != nil {
		return nil, err
	}
	jobId = s.normalizeJobID(jobId)
	state := session.State.Lookup(s.readerView)
	state.SetCriteria(" JobID = ? ", []interface{}{jobId})
	if err = s.reader.Read(ctx, session); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result[0], nil
}
