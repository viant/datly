package lambda

import (
	"context"
	"embed"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/auth/cognito"
	"github.com/viant/datly/gateway/runtime/standalone/handler"
	"github.com/viant/datly/visitor"
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
	if err := handleRequest(writer, httpRequest); err != nil {
		return nil, err
	}
	return adapter.NewResponse(writer), nil
}

func handleRequest(writer http.ResponseWriter, httpRequest *http.Request) error {

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
	if _, err = InitAuthService(config); err != nil {
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

	if strings.HasSuffix(httpRequest.RequestURI, ".ico") {
		writer.WriteHeader(http.StatusNotFound)
	} else {
		httpHandler(writer, httpRequest)
	}
	return nil
}

var authService *cognito.Service
var authServiceInit sync.Once

//go:embed resource/*
var embedFs embed.FS

func InitAuthService(config *gateway.Config) (*cognito.Service, error) {
	if config.Cognito == nil {
		return nil, nil
	}
	fs := afs.New()
	var err error
	authServiceInit.Do(func() {
		if authService, err = cognito.New(config.Cognito, fs, &embedFs); err == nil {
			codec := visitor.Codec(authService)
			registry.Codecs.Register(visitor.New(registry.CodecKeyJwtClaim, codec))
		}

	})
	if err != nil {
		authServiceInit = sync.Once{}
		authService = nil
		return nil, err
	}
	return authService, nil
}
