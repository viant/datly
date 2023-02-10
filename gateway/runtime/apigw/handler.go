package apigw

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/datly/auth/jwt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/gateway"
	"net/http"
	"time"

	"github.com/viant/datly/gateway/runtime/apigw/adapter"
	"github.com/viant/datly/router/proxy"
	"os"
	"sync"
)

var gwayConfig *gateway.Config
var configInit sync.Once

func HandleRequest(ctx context.Context, request *adapter.Request) (*events.APIGatewayProxyResponse, error) {

	writer := proxy.NewWriter()
	if err := HandleHttpRequest(writer, request); err != nil {
		return nil, err
	}

	return adapter.NewResponse(writer), nil
}

func HandleHttpRequest(writer http.ResponseWriter, apiRequest *adapter.Request) error {
	now := time.Now()

	configURL := os.Getenv("CONFIG_URL")
	if configURL == "" {
		return fmt.Errorf("config was emty")
	}
	var err error
	configInit.Do(func() {
		gwayConfig, err = gateway.NewConfigFromURL(context.Background(), configURL)
	})

	if err != nil {
		configInit = sync.Once{}
		return err
	}

	var authService gateway.Authorizer
	if jwtAuth, err := jwt.Init(gwayConfig, nil); err == nil {
		authService = jwtAuth
	} else {
		return err
	}

	service, err := gateway.SingletonWithConfig(gwayConfig, nil, authService, config.Config, nil)
	if err != nil {
		return err
	}
	httpRequest := apiRequest.Request(service.JWTSigner)
	service.LogInitTimeIfNeeded(now, writer)
	service.ServeHTTP(writer, httpRequest)
	return nil
}
