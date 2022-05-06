package app

import (
	"context"
	"github.com/viant/datly/gateway"
	"sync"
)

var service *gateway.Service
var once sync.Once

func Singleton(configURL string) (*gateway.Service, error) {
	var err error
	once.Do(func() {
		ctx := context.Background()
		var config *gateway.Config
		if config, err = gateway.NewConfigFromURL(ctx, configURL); err != nil {
			return
		}
		service, err = gateway.New(ctx, config)
	})
	if err != nil {
		once = sync.Once{}
	}
	return service, err
}
