package db

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/datly/config"
	"github.com/viant/dsc"
	"sync"
)

//Service represents database/datastore service
type Service interface {
	Manager(ctx context.Context, name string) (dsc.Manager, error)
}

type service struct {
	config   *config.Config
	mux      *sync.RWMutex
	registry map[string]dsc.Manager
}

func (s *service) Manager(ctx context.Context, name string) (dsc.Manager, error) {
	s.mux.RLock()
	manager, ok := s.registry[name]
	s.mux.RUnlock()
	if ok {
		return manager, nil
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	var err error
	if manager, err = s.newManager(ctx, name); err != nil {
		return nil, err
	}
	s.registry[name] = manager
	return manager, nil
}

func (s *service) newManager(ctx context.Context, name string) (dsc.Manager, error) {
	connector, err := s.config.Connectors.Get(name)
	if err != nil {
		return nil, err
	}
	manager, err := dsc.NewManagerFactory().Create(connector.Config)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get manager for %v", name)
	}
	return manager, nil
}

//New creates a new database service
func New(config *config.Config) Service {
	return &service{
		config:   config,
		mux:      &sync.RWMutex{},
		registry: make(map[string]dsc.Manager),
	}
}
