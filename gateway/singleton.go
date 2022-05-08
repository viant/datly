package gateway

import (
	"context"
	"github.com/viant/datly/data"
	"github.com/viant/datly/visitor"
	"github.com/viant/gmetric"
	"sync"
)

var service *Service
var once sync.Once

func Singleton(configURL string, visitors visitor.Visitors, types data.Types, metric *gmetric.Service) (*Service, error) {
	var err error
	once.Do(func() {
		ctx := context.Background()
		var config *Config
		if config, err = NewConfigFromURL(ctx, configURL); err != nil {
			return
		}
		service, err = New(ctx, config, visitors, types, metric)
	})
	if err != nil {
		once = sync.Once{}
	}
	return service, err
}

var onceWithConfig sync.Once

func SingletonWithConfig(config *Config, visitors visitor.Visitors, types data.Types, metric *gmetric.Service) (*Service, error) {
	var err error
	onceWithConfig.Do(func() {
		ctx := context.Background()
		service, err = New(ctx, config, visitors, types, metric)
	})
	if err != nil {
		onceWithConfig = sync.Once{}
	}
	return service, err
}
