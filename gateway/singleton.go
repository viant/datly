package gateway

import (
	"context"
	"github.com/viant/datly/config"
	"github.com/viant/gmetric"
	"net/http"
	"sync"
)

var service *Service
var once sync.Once

func Singleton(configURL string, statusHandler http.Handler, authorizer Authorizer, registry *config.Registry, metric *gmetric.Service) (*Service, error) {
	var err error
	once.Do(func() {
		ctx := context.Background()
		var config *Config
		if config, err = NewConfigFromURL(ctx, configURL); err != nil {
			return
		}
		service, err = New(ctx, config, statusHandler, authorizer, registry, metric)
	})
	if err != nil {
		once = sync.Once{}
	}

	return service, err
}

func SingletonWithConfig(config *Config, statusHandler http.Handler, authorizer Authorizer, registry *config.Registry, metric *gmetric.Service) (*Service, error) {
	var err error

	once.Do(func() {
		ctx := context.Background()
		service, err = New(ctx, config, statusHandler, authorizer, registry, metric)
	})

	if err != nil {
		once = sync.Once{}
	}

	return service, err
}

func ResetSingleton() {
	once = sync.Once{}
	if service != nil {
		_ = service.Close()
	}
}
