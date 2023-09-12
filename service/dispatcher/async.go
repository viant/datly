package dispatcher

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/service/reader/handler"
	"github.com/viant/datly/service/session"
	"github.com/viant/toolbox"
	"github.com/viant/xdatly/handler/async"
	"time"
)

func (s *Service) IsEventInvocation(ctx context.Context) bool {
	invocation := s.InvocationType(ctx)
	return invocation == async.InvocationTypeEvent
}

func (s *Service) updateJobStatusRunning(ctx context.Context, component *repository.Component) error {
	if !s.IsEventInvocation(ctx) {
		return nil
	}
	job := s.Job(ctx)
	job.Status = string(async.StatusRunning)
	startedAt := time.Now()
	job.StartTime = &startedAt
	job.WaitTimeMcs = int(startedAt.Sub(job.CreationTime).Microseconds())
	return component.Async.UpdateJob(ctx, job)
}

func (s *Service) updateJobStatusDone(ctx context.Context, aComponent *repository.Component, response *handler.Response) error {
	if !s.IsEventInvocation(ctx) {
		return nil
	}
	job := s.Job(ctx)
	job.Status = string(async.StatusDone)
	endedAt := time.Now()
	job.EndTime = &endedAt
	elapsed := endedAt.Sub(*job.StartTime)
	job.RunTimeMcs = int(elapsed.Microseconds())
	expiryTime := endedAt.Add(aComponent.Async.TTL())
	job.ExpiryTime = &expiryTime
	metrics, _ := json.Marshal(response.Metrics)
	job.Metrics = string(metrics)
	if len(response.Metrics) > 0 && len(response.Metrics[0].Execution.Template) > 0 {
		if cacheStat := response.Metrics[0].Execution.Template[0].CacheStats; cacheStat != nil {
			job.CacheNamespace = &cacheStat.Namespace
			job.CacheSet = &cacheStat.Dataset
			job.CacheKey = &cacheStat.Key
		}
		for _, metric := range response.Metrics {
			if pSQL := metric.ParametrizedSQL(); len(pSQL) > 0 {
				job.SQL = append(job.SQL, &async.SQL{Query: pSQL[0].Query, Args: pSQL[0].Args})
			}
		}
	}
	return aComponent.Async.UpdateJob(ctx, job)
}

func (s *Service) buildJob(ctx context.Context, aSession *session.Session, aComponent *repository.Component, jobRef string, options *session.Options) (*async.Job, error) {
	asyncModule := aComponent.Async
	encodedState, err := aSession.MarshalJSON()
	if err != nil {
		return nil, err
	}
	UUID, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	job := &async.Job{
		ID:           UUID.String(),
		Ref:          jobRef,
		Status:       string(async.StatusPending),
		Request:      async.Request{State: string(encodedState)},
		MainView:     aComponent.View.Name,
		CreationTime: time.Now(),
		JobType:      string(aComponent.Service),
	}
	if asyncModule.UserID != nil {
		value, _, err := aSession.LookupValue(ctx, asyncModule.UserID, options)
		if err != nil {
			return nil, err
		}
		userID := toolbox.AsString(value)
		job.UserID = &userID
	}
	if asyncModule.UserEmail != nil {
		value, _, err := aSession.LookupValue(ctx, asyncModule.UserEmail, options)
		if err != nil {
			return nil, err
		}
		userEmail := value.(string)
		job.UserEmail = &userEmail
	}
	job.Method = aComponent.Path.Method
	job.URI = aComponent.Path.URI
	return job, nil
}
