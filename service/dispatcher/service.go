package dispatcher

import (
	"context"
	"fmt"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/service"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/utils/httputils"
)

type Service struct{}

func (s *Service) Dispatch(ctx context.Context, aComponent *repository.Component, aSession *session.Session) (interface{}, error) {
	switch aComponent.Service {
	case service.TypeReader:
		return s.runQuery(ctx, aComponent, aSession)
	case service.TypeExecutor:
		return s.execute(ctx, aComponent, aSession)
	}
	return nil, httputils.NewHttpMessageError(500, fmt.Errorf("unsupported Type %v", aComponent.Service))
}
