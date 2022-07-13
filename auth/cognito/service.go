package cognito

import (
	"context"
	"embed"
	"github.com/viant/afs"
	"github.com/viant/datly/codec"
	"github.com/viant/scy/auth/cognito"
)

type Service struct {
	Config *Config
	*cognito.Service
	fs  afs.Service
	efs *embed.FS
}

func (s *Service) Valuer() codec.Valuer {
	return codec.NewValuer(s.Value)
}

func (s *Service) Name() string {
	//TODO implement me
	panic("implement me")
}

func New(config *Config, fs afs.Service, efs *embed.FS) (*Service, error) {
	cognito, err := cognito.New(context.Background(), &config.Config)
	if err != nil {
		return nil, err
	}
	return &Service{
		Config:  config,
		Service: cognito,
		fs:      fs,
		efs:     efs,
	}, nil
}
