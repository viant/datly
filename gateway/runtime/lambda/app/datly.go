package main

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/viant/datly/gateway/runtime/lambda/adapter"
	"github.com/viant/toolbox"
)

func main() {
	lambda.Start(handleRequest)
}

func handleRequest(ctx context.Context, request *adapter.Request) (*events.LambdaFunctionURLResponse, error) {
	toolbox.Dump(request)
	response := &events.LambdaFunctionURLResponse{}
	response.Body = "Hello from datly"
	response.StatusCode = 200
	return response, nil
}
