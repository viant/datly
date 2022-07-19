package lambda

import (
	"context"
	"fmt"
	"github.com/viant/datly/auth/jwt"
	"github.com/viant/datly/gateway/runtime/standalone/handler"
	"net/http"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/datly/gateway"

	"github.com/viant/datly/gateway/registry"
	"github.com/viant/datly/gateway/runtime/lambda/adapter"
	"github.com/viant/datly/router/proxy"
	"os"
	"sync"
)

var config *gateway.Config
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
	var authenticator jwt.Authenticator
	if _, err = jwt.Init(config, nil); err != nil {
		return err
	}
	service, err := gateway.SingletonWithConfig(config, registry.Codecs, registry.Types, nil)
	if err != nil {
		return err
	}
	httpHandler := service.Handle
	if authenticator != nil {
		httpHandler = authenticator.Auth(service.Handle)
	}
	if err != nil {
		return err
	}
	if strings.Contains(httpRequest.RequestURI, config.Meta.ViewURI) {
		viewHandler := handler.NewView(config.Meta.ViewURI, &config.Meta, service.View)
		viewHandler.ServeHTTP(writer, httpRequest)
		return nil
	}

	if strings.HasSuffix(httpRequest.RequestURI, ".ico") {
		writer.WriteHeader(http.StatusNotFound)
	} else {
		httpHandler(writer, httpRequest)
	}
	return nil
}
