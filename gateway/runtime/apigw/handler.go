package apigw

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/datly/auth/jwt"
	"github.com/viant/datly/gateway"
	"net/http"
	"time"

	"github.com/viant/datly/gateway/registry"
	"github.com/viant/datly/gateway/runtime/apigw/adapter"
	"github.com/viant/datly/router/proxy"
	"os"
	"sync"
)

var config *gateway.Config
var configInit sync.Once

func HandleRequest(ctx context.Context, request *adapter.Request) (*events.APIGatewayProxyResponse, error) {
	httpRequest := request.Request()
	writer := proxy.NewWriter()
	if err := HandleHttpRequest(writer, httpRequest); err != nil {
		return nil, err
	}

	return adapter.NewResponse(writer), nil
}

func HandleHttpRequest(writer http.ResponseWriter, httpRequest *http.Request) error {
	now := time.Now()

	configURL := os.Getenv("CONFIG_URL")
	if configURL == "" {
		return fmt.Errorf("config was emty")
	}
	var err error
	configInit.Do(func() {
		config, err = gateway.NewConfigFromURL(context.Background(), configURL)
	})

	if err != nil {
		configInit = sync.Once{}
		return err
	}

	var authService gateway.Authorizer
	if jwtAuth, err := jwt.Init(config, nil); err == nil {
		authService = jwtAuth
	} else {
		return err
	}

	service, err := gateway.SingletonWithConfig(config, nil, authService, registry.Codecs, registry.Types, nil)
	if err != nil {
		return err
	}

	service.LogInitTimeIfNeeded(now, writer)
	service.ServeHTTP(writer, httpRequest)
	return nil
}
