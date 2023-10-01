package apigw

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/repository/extension"
	"github.com/viant/datly/service/auth/jwt"
	"net/http"
	"time"

	"github.com/viant/datly/gateway/router/proxy"
	"github.com/viant/datly/gateway/runtime/apigw/adapter"
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
	fs := gateway.NewFs(configURL)
	configInit.Do(func() {
		gwayConfig, err = gateway.NewConfigFromURL(context.Background(), fs, configURL)
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

	service, err := gateway.SingletonWithConfig(gwayConfig, nil, authService, extension.Config, nil)
	if err != nil {
		return err
	}
	httpRequest := apiRequest.Request(service.JWTSigner)
	service.LogInitTimeIfNeeded(now, writer)
	service.ServeHTTP(writer, httpRequest)
	return nil
}
