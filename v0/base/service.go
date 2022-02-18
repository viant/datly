package base

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/datly/v0/base/contract"
	"github.com/viant/datly/v0/binder"
	config2 "github.com/viant/datly/v0/config"
	data2 "github.com/viant/datly/v0/data"
	"github.com/viant/datly/v0/db"
	dbservice "github.com/viant/datly/v0/db/service"
	"github.com/viant/datly/v0/matcher"
	"github.com/viant/datly/v0/metric"
	"github.com/viant/dsc"
)

//Service represents base service
type Service interface {
	binder.Service
	db.Service
	matcher.Service
	Config() *config2.Config
}

type service struct {
	matcher.Service
	binder  binder.Service
	db      db.Service
	_config *config2.Config
}

//BuildDataPool build data pool
func (s *service) BuildDataPool(ctx context.Context, request contract.Request, view *data2.View, rule *config2.Rule, metrics *metric.Metrics, filterType ...string) (data2.Pool, error) {
	return s.binder.BuildDataPool(ctx, request, view, rule, metrics, filterType...)
}

//Manager returns db manager
func (s *service) Manager(ctx context.Context, name string) (dsc.Manager, error) {
	return s.db.Manager(ctx, name)
}

//Config returns config
func (s *service) Config() *config2.Config {
	return s._config
}

//New creates a new base service
func New(ctx context.Context, config *config2.Config) (Service, error) {
	matcher, err := matcher.New(ctx, config)
	if err != nil {
		return nil, err
	}
	dbService := dbservice.New(config, afs.New())
	return &service{
		Service: matcher,
		db:      dbService,
		binder:  binder.New(dbService),
	}, err
}
