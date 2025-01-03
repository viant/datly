package serverless

import (
	"context"
	"embed"
	"fmt"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/service/auth/jwt"
	"github.com/viant/datly/view/extension"
	"os"
)

var gatewayConfig *gateway.Config

var _service *gateway.Service

func GetService() (*gateway.Service, error) {
	if _service != nil {
		return _service, nil
	}
	configURL := os.Getenv("CONFIG_URL")
	if configURL == "" {
		return nil, fmt.Errorf("config was emty")
	}
	service, err := gateway.Singleton(context.Background(),
		gateway.WithInitializer(func(config *gateway.Config, fs *embed.FS) error {
			return jwt.Init(gatewayConfig, fs)
		}),
		gateway.WithConfigURL(configURL),
		gateway.WithExtensions(extension.Config))
	if err != nil {
		return nil, err
	}
	_service = service
	return service, nil
}
