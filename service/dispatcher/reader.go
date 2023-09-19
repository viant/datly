package dispatcher

import (
	"context"
	"fmt"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/service/reader/handler"
	"github.com/viant/datly/service/session"
	"github.com/viant/xdatly/handler/async"
)

func (s *Service) runQuery(ctx context.Context, component *repository.Component, aSession *session.Session) (interface{}, error) {
	//TODO handler async
	fmt.Printf("run query\n")
	readerHandler := handler.New(component.Output.Type.Type(), &component.Output.Type)
	var options = []reader.Option{
		reader.WithIncludeSQL(true),
		reader.WithCacheDisabled(false),
	}
	s.adjustAsyncOptions(ctx, &options)
	response := readerHandler.Handle(ctx, component.View, aSession, options...)
	if err := s.updateJobStatusDone(ctx, component, response); err != nil {
		return nil, err
	}
	return response.Output, response.Error
}

// adjustAsyncOptions function adjust reading option to dryRun when asyb job is scheduled but not yet completed
func (s *Service) adjustAsyncOptions(ctx context.Context, options *[]reader.Option) {
	if job := s.Job(ctx); job != nil {
		if s.IsEventInvocation(ctx) {
			//Makes sure cache is always refreshed
			*options = append(*options, reader.WithCacheRefresh())
		} else if async.Status(job.Status) != async.StatusDone {
			//Make sure not actual database is used
			*options = append(*options, reader.WithDryRun())
		}
	}

}

func (s *Service) InvocationType(ctx context.Context) async.InvocationType {
	if value := ctx.Value(async.InvocationTypeKey); value != nil {
		ret, ok := value.(async.InvocationType)
		if ok {
			return ret
		}
	}
	return async.InvocationTypeUndefined
}

func (s *Service) Job(ctx context.Context) *async.Job {
	if value := ctx.Value(async.JobKey); value != nil {
		ret, ok := value.(*async.Job)
		if ok {
			return ret
		}
	}
	return nil
}
