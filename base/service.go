package base

import (
	"context"
	"github.com/viant/datly/base/contract"
	"github.com/viant/datly/binder"
	"github.com/viant/datly/config"
	"github.com/viant/datly/data"
	"github.com/viant/datly/db"
	"github.com/viant/datly/db/manager"
	"github.com/viant/datly/matcher"
	"github.com/viant/datly/metric"
	"github.com/viant/dsc"
)

//Service represents base service
type Service interface {
	binder.Service
	db.Service
	matcher.Service
	Config() *config.Config
}

type service struct {
	matcher.Service
	binder  binder.Service
	db      db.Service
	_config *config.Config
}

//BuildDataPool build data pool
func (s *service) BuildDataPool(ctx context.Context, request contract.Request, view *data.View, rule *config.Rule, metrics *metric.Metrics, filterType ... string) (data.Pool, error) {
	return s.binder.BuildDataPool(ctx, request, view, rule, metrics, filterType...)
}

//Manager returns db manager
func (s *service) Manager(ctx context.Context, name string) (dsc.Manager, error) {
	return s.db.Manager(ctx, name)
}

//Config returns config
func (s *service) Config() *config.Config {
	return s._config
}

//New creates a new base service
func New(ctx context.Context, config *config.Config) (Service, error) {
	matcher, err := matcher.New(ctx, config)
	if err != nil {
		return nil, err
	}
	dbService := manager.New(config)
	return &service{
		Service: matcher,
		db:      dbService,
		binder:  binder.New(dbService),
	}, err
}
