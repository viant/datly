package jobs

import (
	"context"
	"github.com/viant/datly/service/reader"
	"github.com/viant/xdatly/handler/async"
	"time"
)

const (
	viewID = "datly_jobs"
)

func (s *Service) JobById(ctx context.Context, jobID string) (*async.Job, error) {
	var result = []*async.Job{}
	session, err := reader.NewSession(&result, s.readerView, reader.WithDryRun())
	if err != nil {
		return nil, err
	}
	state := session.State.Lookup(s.readerView)
	state.SetCriteria(" ID = ? ", []interface{}{jobID})
	if err = s.reader.Read(ctx, session); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result[0], nil
}

func (s *Service) JobByMatchKey(ctx context.Context, matchKey string, ttl time.Duration) (*async.Job, error) {
	var result = []*async.Job{}
	session, err := reader.NewSession(&result, s.readerView)
	if err != nil {
		return nil, err
	}
	state := session.State.Lookup(s.readerView)
	createdAt := time.Now().Add(-ttl).UTC()
	state.SetCriteria("  MatchKey = ? AND ((CreationTime >= ? AND Status <> ?) OR (CreationTime >= ? AND Status = ?))", []interface{}{matchKey, createdAt, async.StatusError, createdAt, async.StatusError})
	if err = s.reader.Read(ctx, session); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return s.matchFailedJob(matchKey)
	}
	return result[0], nil
}
