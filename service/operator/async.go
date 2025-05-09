package operator

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/service/reader/handler"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view/state"
	"github.com/viant/structology"
	"github.com/viant/toolbox"
	"github.com/viant/xdatly/handler/async"
	"reflect"
	"strings"
	"time"
)

func (s *Service) IsEventInvocation(ctx context.Context) bool {
	invocation := s.InvocationType(ctx)
	return invocation == async.InvocationTypeEvent
}

func (s *Service) updateBackgroundJob(ctx context.Context, component *repository.Component) error {
	if !s.IsEventInvocation(ctx) {
		return nil
	}
	job := s.Job(ctx)
	job.Status = string(async.StatusRunning)
	startedAt := time.Now()
	job.StartTime = &startedAt
	job.WaitTimeInMcs = int(startedAt.Sub(job.CreationTime).Microseconds())
	return component.Async.UpdateJob(ctx, job)
}

func (s *Service) updateJobStatusDone(ctx context.Context, aComponent *repository.Component, response *handler.Response, syncFlag bool, startTime time.Time) error {
	if !s.IsEventInvocation(ctx) && !syncFlag {
		return nil
	}
	job := s.Job(ctx)
	if job == nil {
		return nil
	}
	job.Status = string(async.StatusDone)
	if job.StartTime == nil || syncFlag {
		job.StartTime = &startTime
	}
	var expiryTime time.Time
	endedAt := time.Now()
	job.EndTime = &endedAt

	if response.Error != nil {
		errMessage := response.Error.Error()
		job.Status = string(async.StatusError)
		job.Error = &errMessage
		expiryTime = endedAt.Add(aComponent.Async.ErrorTTL())
	} else {
		expiryTime = endedAt.Add(aComponent.Async.TTL())
	}

	job.ExpiryTime = &expiryTime
	elapsed := endedAt.Sub(*job.StartTime)
	job.RunTimeInMcs = int(elapsed.Microseconds())
	metrics, _ := json.Marshal(response.Metrics)
	job.Metrics = string(metrics)
	if len(response.Metrics) > 0 && len(response.Metrics[0].Executions) > 0 {
		if cacheStat := response.Metrics[0].Executions[0].CacheStats; cacheStat != nil {
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

var queryParameter = state.NewParameter("Query", state.NewQueryLocation(""), state.WithParameterSchema(state.NewSchema(reflect.TypeOf(""))))
var textLimit = 63 * 1024

func (s *Service) buildJob(ctx context.Context, aSession *session.Session, aState *structology.State, aComponent *repository.Component, matchKey string, options *session.Options) (*async.Job, error) {
	asyncModule := aComponent.Async

	query := struct {
		URI string `parameter:"kind=query"`
	}{}
	_ = aSession.Bind(ctx, &query)
	_ = aSession.SetCacheValue(ctx, queryParameter, query.URI)

	encodedState, err := aSession.MarshalJSON()
	if err != nil {
		return nil, err
	}
	if len(encodedState) > textLimit {
		encodedState = encodedState[:textLimit]
	}
	UUID, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	job := &async.Job{
		ID:           UUID.String(),
		MatchKey:     matchKey,
		Status:       string(async.StatusPending),
		Request:      async.Request{State: string(encodedState)},
		MainView:     aComponent.View.Name,
		Module:       aComponent.View.Module,
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
		if value == nil {
			value = ""
		}
		userEmail := value.(string)
		job.UserEmail = &userEmail
	}

	job.Method = aComponent.Path.Method
	job.URI = s.expandURI(ctx, aSession, aComponent, options)
	return job, nil
}

func (s *Service) expandURI(ctx context.Context, aSession *session.Session, aComponent *repository.Component, options *session.Options) string {
	URI := aComponent.URI
	for i := 0; i < strings.Count(URI, "{"); i++ {
		index := strings.Index(URI, "{")
		if index == -1 {
			break
		}
		end := strings.Index(URI, "}")
		key := URI[index+1 : end]
		uriParameter := aComponent.Input.Type.Parameters.LookupByLocation(state.KindPath, key)
		if uriParameter != nil {
			value, _, err := aSession.LookupValue(ctx, uriParameter, options)
			if err == nil {
				URI = strings.Replace(URI, "{"+key+"}", toolbox.AsString(value), 1)
			}
		}
	}
	return URI
}
