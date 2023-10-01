package gateway

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/repository/extension"
	"github.com/viant/gmetric"
	"net/http"
	"os"
	"sync"
)

var service *Service
var once sync.Once

func Singleton(configURL string, statusHandler http.Handler, authorizer Authorizer, registry *extension.Registry, metric *gmetric.Service) (*Service, error) {
	var err error
	fs := NewFs(configURL)
	once.Do(func() {
		ctx := context.Background()
		var config *Config
		if config, err = NewConfigFromURL(ctx, fs, configURL); err != nil {
			return
		}
		service, err = New(ctx, config, statusHandler, authorizer, registry, metric)
	})
	if err != nil {
		once = sync.Once{}
	}

	return service, err
}

func NewFs(configURL string) afs.Service {
	if os.Getenv("DATLY_FS") == "cfs" {
		ParentURL, _ := url.Split(configURL, file.Scheme)
		return NewCacheFs(ParentURL)
	}
	return afs.New()
}

func SingletonWithConfig(config *Config, statusHandler http.Handler, authorizer Authorizer, registry *extension.Registry, metric *gmetric.Service) (*Service, error) {
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
