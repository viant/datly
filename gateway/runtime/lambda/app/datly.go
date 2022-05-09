package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/viant/datly/gateway/runtime/lambda/adapter"
)

func main() {
	lambda.Start(handleRequest)
}

func handleRequest(ctx context.Context, request *adapter.Request) (*adapter.Response, error) {

	return nil, fmt.Errorf("not supported yet")
}
