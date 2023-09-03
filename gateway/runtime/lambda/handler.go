package lambda

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/datly/config"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/router/proxy"
	"github.com/viant/datly/gateway/runtime/lambda/adapter"
	"github.com/viant/datly/service/auth/jwt"
	async2 "github.com/viant/xdatly/handler/async"
	"net/http"
	"os"
	"sync"
	"time"
)

var gwayConfig *gateway.Config
var configInit sync.Once

func HandleRequest(ctx context.Context, request *adapter.Request) (*events.LambdaFunctionURLResponse, error) {
	httpRequest := request.Request()
	writer := proxy.NewWriter()
	if err := HandleHttpRequest(writer, httpRequest); err != nil {
		return nil, err
	}
	return adapter.NewResponse(writer), nil
}

func HandleHttpRequest(writer http.ResponseWriter, httpRequest *http.Request) error {
	service, err := prepareHandler(writer)
	if err != nil {
		return err
	}
	service.ServeHTTP(writer, httpRequest)
	return nil
}

func HandleHTTPAsyncRequest(writer http.ResponseWriter, httpRequest *http.Request, record *async2.Job) error {
	handler, err := prepareHandler(writer)
	if err != nil {
		return err
	}

	handler.ServeHTTPAsync(writer, httpRequest, record)

	return nil
}

func prepareHandler(writer http.ResponseWriter) (*gateway.Service, error) {
	now := time.Now()

	configURL := os.Getenv("CONFIG_URL")
	if configURL == "" {
		return nil, fmt.Errorf("config was emty")
	}

	var err error
	fs := gateway.NewFs(configURL)
	configInit.Do(func() {
		gwayConfig, err = gateway.NewConfigFromURL(context.Background(), fs, configURL)
	})

	if err != nil {
		configInit = sync.Once{}
		return nil, err
	}

	var authorizer gateway.Authorizer
	if jwtAuthorizer, err := jwt.Init(gwayConfig, nil); err == nil {
		authorizer = jwtAuthorizer
	} else {
		return nil, err
	}

	service, err := gateway.SingletonWithConfig(gwayConfig, nil, authorizer, config.Config, nil)
	if err != nil {
		return nil, err
	}

	service.LogInitTimeIfNeeded(now, writer)
	return service, nil
}
