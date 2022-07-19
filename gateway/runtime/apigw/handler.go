package apigw

import (
	"context"
	"fmt"
	"github.com/viant/datly/auth/jwt"
	"github.com/viant/datly/gateway/runtime/standalone/handler"
	"github.com/viant/datly/router/openapi3"
	"net/http"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/datly/gateway"

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
	var authService jwt.Authenticator
	if authService, err = jwt.Init(config, nil); err != nil {
		return err
	}
	service, err := gateway.SingletonWithConfig(config, registry.Codecs, registry.Types, nil)
	if err != nil {
		return err
	}
	httpHandler := service.Handle
	if authService != nil {
		httpHandler = authService.Auth(service.Handle)
	}
	if err != nil {
		return err
	}
	if strings.Contains(httpRequest.RequestURI, config.Meta.ViewURI) {
		viewHandler := handler.NewView(config.Meta.ViewURI, &config.Meta, service.View)
		viewHandler.ServeHTTP(writer, httpRequest)
		return nil
	}
	if strings.Contains(httpRequest.RequestURI, config.Meta.ConfigURI) {
		viewHandler := handler.NewConfig(config, nil, &config.Meta)
		viewHandler.ServeHTTP(writer, httpRequest)
		return nil
	}
	if strings.Contains(httpRequest.RequestURI, config.Meta.OpenApiURI) {
		//TODO: add openapi3.Info to Config
		openApiHandler := handler.NewOpenApi(config.APIPrefix, config.Meta.OpenApiURI, openapi3.Info{}, service.Routes)
		openApiHandler.ServeHTTP(writer, httpRequest)
		return nil
	}
	if strings.HasSuffix(httpRequest.RequestURI, ".ico") {
		writer.WriteHeader(http.StatusNotFound)
	} else {
		httpHandler(writer, httpRequest)
	}
	return nil
}
