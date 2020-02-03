package service

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/datly/secret/access"
	"github.com/viant/datly/secret/access/ssm"
	"github.com/viant/datly/secret/access/storage"
)

type service struct {fs afs.Service}

//Access returns access secrets or error
func (s *service) Access(ctx context.Context, request *access.Request) ([]byte, error) {
	var srv access.Service
	var err error
	switch request.Method {
	case access.MethodAWSSystemManager:
		srv, err = ssm.New()
	case access.MethodStorage:
		srv = storage.New(s.fs)
	default:
		err = errors.Errorf("unsupported method: %v", request.Method)
	}
	if err != nil {
		return nil, err
	}
	return srv.Access(ctx, request)
}

//New creates a new access service
func New(fs afs.Service) access.Service {
	return &service{fs}
}