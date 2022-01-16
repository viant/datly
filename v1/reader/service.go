package reader

import (
	"context"
	"github.com/viant/datly/v1/config"
	"github.com/viant/datly/v1/connection"
	"github.com/viant/datly/v1/data"
)

type Service struct {
	connection *connection.Service
}

func (s *Service) Read(ctx context.Context, view *data.View, dest interface{}) error {
	return nil
}

func New(connectors ...*config.Connector) (*Service, error) {
	conn, err := connection.New(connectors...)
	if err != nil {
		return nil, err
	}
	return &Service{connection: conn}, nil
}
