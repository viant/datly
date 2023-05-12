package datly

import (
	"context"
	"fmt"
	"github.com/viant/datly/auth/jwt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/executor"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/view"
	sjwt "github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	"sync/atomic"
)

type (
	Service struct {
		initialized int32
		reader      *reader.Service
		executor    *executor.Executor
		jwtVerifier *verifier.Service
		config      *Config
		connector   *view.Connector
	}

	Config struct {
		Connector    *view.Connector
		Connectors   []*view.Connector
		JWTValidator *verifier.Config
	}
)

//GetBegin
func (s *Service) Read(ctx context.Context, viewId string, dest interface{}, option ...reader.Option) error {
	return s.reader.ReadInto(ctx, viewId, dest, option...)
}

func (s *Service) Exec(ctx context.Context, viewId string, options ...executor.Option) error {
	execView, err := s.reader.Resource.View(viewId)
	if err != nil {
		return err
	}
	return s.executor.Execute(ctx, execView, options...)
}

func (s *Service) VerifyClaims(ctx context.Context, tokenString string) (*sjwt.Claims, error) {
	if s.jwtVerifier == nil {
		return nil, fmt.Errorf("jwtVerifier was not configuered")
	}
	return s.jwtVerifier.VerifyClaims(ctx, tokenString)
}

//AddViews adds view
func (s *Service) AddViews(views ...*view.View) error {
	if err := s.ensureNotInitialised(); err != nil {
		return err
	}
	s.reader.Resource.AddViews(views...)
	return nil
}

//View returns registered view
func (s *Service) View(name string) (*view.View, error) {
	return s.reader.Resource.View(name)
}

//Connector returns registered connector or default connector
func (s *Service) Connector(name string) (*view.Connector, error) {
	if name == "" && s.connector != nil {
		return s.connector, nil
	}
	return s.reader.Resource.Connector(name)
}

//AddParameter add global parameters
func (s *Service) AddParameter(parameters ...*view.Parameter) error {
	if err := s.ensureNotInitialised(); err != nil {
		return err
	}
	s.reader.Resource.AddParameters(parameters...)
	return nil
}

//AddConnectors adds connectors
func (s *Service) AddConnectors(connectors ...*view.Connector) error {
	if err := s.ensureNotInitialised(); err != nil {
		return err
	}
	s.reader.Resource.AddConnectors(connectors...)
	return nil
}

func (s *Service) ensureNotInitialised() error {
	if atomic.LoadInt32(&s.initialized) == 1 {
		return fmt.Errorf("can not update resource after server was initialised")
	}
	return nil
}

//Init initialises service
func (s *Service) Init(ctx context.Context, options ...interface{}) error {
	if !atomic.CompareAndSwapInt32(&s.initialized, 0, 1) {
		return fmt.Errorf("already initialised")
	}
	if s.jwtVerifier != nil {
		if err := s.jwtVerifier.Init(context.Background()); err == nil {
			var codec = jwt.New(config.CodecKeyJwtClaim, s.jwtVerifier.VerifyClaims)
			config.Config.RegisterCodec(codec)
		}
	}
	return s.reader.Resource.Init(ctx, options...)
}

//New creates a datly service
func New(cfg *Config) *Service {
	ret := &Service{
		config:    cfg,
		reader:    reader.New(),
		executor:  executor.New(),
		connector: cfg.Connector,
	}
	if cfg.Connector != nil {
		_ = ret.AddConnectors(cfg.Connector)
	}
	if cfg.JWTValidator != nil {
		ret.jwtVerifier = verifier.New(cfg.JWTValidator)
	}
	return ret
}
