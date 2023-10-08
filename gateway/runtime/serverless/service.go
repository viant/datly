package serverless

import (
	"context"
	"fmt"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/service/auth/jwt"
	"github.com/viant/datly/view/extension"
	"os"
	"sync"
)

var gatewayConfig *gateway.Config
var configInit sync.Once

var _service *gateway.Service

func GetService() (*gateway.Service, error) {
	if _service != nil {
		return _service, nil
	}
	configURL := os.Getenv("CONFIG_URL")
	if configURL == "" {
		return nil, fmt.Errorf("config was emty")
	}

	var err error
	fs := gateway.NewFs(configURL)
	configInit.Do(func() {
		gatewayConfig, err = gateway.NewConfigFromURL(context.Background(), fs, configURL)
	})

	if err != nil {
		configInit = sync.Once{}
		return nil, err
	}

	var authorizer gateway.Authorizer
	if jwtAuthorizer, err := jwt.Init(gatewayConfig, nil); err == nil {
		authorizer = jwtAuthorizer
	} else {
		return nil, err
	}

	service, err := gateway.SingletonWithConfig(gatewayConfig, nil, authorizer, extension.Config, nil)
	if err != nil {
		return nil, err
	}

	_service = service
	return service, nil
}
