package service

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	access2 "github.com/viant/datly/v0/secret/access"
	"github.com/viant/datly/v0/secret/access/ssm"
	"github.com/viant/datly/v0/secret/access/storage"
)

type service struct{ fs afs.Service }

//Access returns access secrets or error
func (s *service) Access(ctx context.Context, request *access2.Request) ([]byte, error) {
	var srv access2.Service
	var err error
	switch request.Method {
	case access2.MethodAWSSystemManager:
		srv, err = ssm.New()
	case access2.MethodStorage:
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
func New(fs afs.Service) access2.Service {
	return &service{fs}
}
