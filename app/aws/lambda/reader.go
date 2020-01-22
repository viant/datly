package lambda

import (
	"context"
	"datly/app/aws/bridge"
	"datly/base"
	"datly/reader"
	"datly/singleton"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/pkg/errors"
	"os"
)

func StartReader() {
	lambda.Start(handleRequest)
}

func handleRequest(ctx context.Context, apiRequest events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	response := events.APIGatewayProxyResponse{}
	err := handleRead(ctx, apiRequest, &response)
	if err != nil {
		return bridge.ErrorAPIGatewayResponse(err), err
	}
	bridge.SetCORSHeaderIfNeeded(&apiRequest, &response)
	return response, nil
}

func handleRead(ctx context.Context, apiRequest events.APIGatewayProxyRequest, response *events.APIGatewayProxyResponse) error {
	config := os.Getenv(base.ConfigKey)
	service, err := singleton.Reader(ctx, config)
	if err != nil {
		return errors.Wrapf(err, "failed to create reader with config: %v", config)
	}
	httpRequest, err := bridge.NewHTTPRequest(&apiRequest)
	if err != nil {
		return  errors.Wrapf(err, "failed to create http.Request")
	}
	writer := bridge.NewHTTPWriter(response)
	handle := reader.HandleRead(service)
	handle(writer, httpRequest)
	return nil
}


