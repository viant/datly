package dispatcher

import (
	"context"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/service/reader/handler"
	"github.com/viant/datly/service/session"
)

func (s *Service) runQuery(ctx context.Context, component *repository.Component, aSession *session.Session) (interface{}, error) {
	//TODO handler async
	readerHandler := handler.New(component.Output.Type.Type(), &component.Output.Type)
	response := readerHandler.Handle(ctx, component.View, aSession,
		reader.WithIncludeSQL(true),
		reader.WithCacheDisabled(false))
	return response.Output, response.Error
}
