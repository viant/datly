package dispatcher

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/service/reader/handler"
	"github.com/viant/datly/service/session"
	"github.com/viant/structology"
	"github.com/viant/xdatly/handler/async"
	"time"
)

func (s *Service) updateJobStatusRunning(ctx context.Context, component *repository.Component) error {
	if s.InvocationType(ctx) != async.InvocationTypeEvent {
		return nil
	}
	job := s.Job(ctx)
	job.Status = string(async.StatusRunning)
	return component.Async.UpdateJob(ctx, job)
}

func (s *Service) updateJobStatusDone(ctx context.Context, component *repository.Component, response *handler.Response) error {
	if s.InvocationType(ctx) != async.InvocationTypeEvent {
		return nil
	}
	job := s.Job(ctx)
	job.Status = string(async.StatusDone)
	endedAt := time.Now()
	job.EndTime = &endedAt
	elapsed := endedAt.Sub(job.CreationTime)
	job.TimeTaken = &elapsed
	metrics, _ := json.Marshal(response.Metrics)
	job.Metrics = string(metrics)
	return component.Async.UpdateJob(ctx, job)
}

func (s *Service) buildJob(ctx context.Context, aSession *session.Session, aComponent *repository.Component, jobId string, aState *structology.State, options *session.Options) (*async.Job, error) {
	asyncModule := aComponent.Async
	encodedState, err := aSession.MarshalJSON()
	if err != nil {
		return nil, err
	}
	job := &async.Job{
		JobID:        jobId,
		Status:       string(async.StatusPending),
		Request:      async.Request{State: string(encodedState)},
		MainView:     aComponent.View.Name,
		CreationTime: time.Now(),
		JobType:      string(aComponent.Service),
	}

	if asyncModule.UserID != nil {
		userID := ""
		if jobId, err = aState.String(asyncModule.UserID.Name); err != nil {
			return nil, err
		}
		job.UserID = &userID
	}
	if asyncModule.UserEmail != nil {
		userEmail := ""
		if userEmail, err = aState.String(asyncModule.UserEmail.Name); err != nil {
			return nil, err
		}
		job.UserEmail = &userEmail
	}

	aRequest, err := aSession.HttpRequest(ctx, options)
	if err != nil {
		return nil, err
	}
	job.Method = aRequest.Method
	job.URI = aRequest.URL.Path
	return job, nil
}
