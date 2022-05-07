package app

import (
	"context"
	"github.com/viant/datly/data"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/visitor"
	"sync"
)

var service *gateway.Service
var once sync.Once

func Singleton(configURL string, visitors visitor.Visitors, types data.Types) (*gateway.Service, error) {
	var err error
	once.Do(func() {
		ctx := context.Background()
		var config *gateway.Config
		if config, err = gateway.NewConfigFromURL(ctx, configURL); err != nil {
			return
		}
		service, err = gateway.New(ctx, config, visitors, types)
	})
	if err != nil {
		once = sync.Once{}
	}
	return service, err
}
