package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/viant/afsc/aws"
	_ "github.com/viant/afsc/gcp"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/afsc/s3"
	_ "github.com/viant/bigquery"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/registry"
	"github.com/viant/datly/gateway/runtime/lambda/adapter"
	"github.com/viant/datly/router/proxy"
	"os"
)

func main() {
	lambda.Start(handleRequest)
}

func handleRequest(ctx context.Context, request *adapter.Request) (*events.LambdaFunctionURLResponse, error) {
	configURL := os.Getenv("CONFIG_URL")
	if configURL == "" {
		return nil, fmt.Errorf("config was emty")
	}
	service, err := gateway.Singleton(configURL, registry.Codecs, registry.Types, nil)
	if err != nil {
		return nil, err
	}
	writer := proxy.NewWriter()
	service.Handle(writer, request.Request())
	return adapter.NewResponse(writer), nil
}
