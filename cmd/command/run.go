package command

import (
	"context"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/service/auth/jwt"
)

func (s *Service) Run(ctx context.Context, run *options.Run) (err error) {
	srv, err := s.run(ctx, run)
	if err != nil {
		return err
	}
	return srv.ListenAndServe()
}

func (s *Service) run(ctx context.Context, run *options.Run) (*standalone.Server, error) {
	var err error
	if s.config, err = standalone.NewConfigFromURL(ctx, run.ConfigURL); err != nil {
		return nil, err
	}
	authenticator, err := jwt.Init(s.config.Config, nil)
	var srv *standalone.Server
	if authenticator == nil {
		srv, err = standalone.New(s.config)
	} else {
		srv, err = standalone.NewWithAuth(s.config, authenticator)
	}
	return srv, err
}
