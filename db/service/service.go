package service

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/datly/config"
	"github.com/viant/datly/db"
	"github.com/viant/datly/secret/access"
	aservice "github.com/viant/datly/secret/access/service"
	"github.com/viant/dsc"
	"github.com/viant/toolbox/cred"
	"sync"
)

type service struct {
	secret   access.Service
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

	if connector.SecuredCredentials != nil && connector.Config.CredConfig == nil {
		accessRequest := access.Request(*connector.SecuredCredentials)
		data, err := s.secret.Access(ctx, &accessRequest)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get secret for connector: %v", name)
		}
		credConfig := &cred.Config{}
		err = json.Unmarshal(data, credConfig)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to decode secret for connector: %v, %s", name, data)
		}
		connector.Config.ApplyCredentials(credConfig)
	}

	manager, err := dsc.NewManagerFactory().Create(connector.Config)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get manager for %v", name)
	}
	return manager, nil
}

//New creates a new database service
func New(config *config.Config, fs afs.Service) db.Service {
	return &service{
		secret:   aservice.New(fs),
		config:   config,
		mux:      &sync.RWMutex{},
		registry: make(map[string]dsc.Manager),
	}
}
