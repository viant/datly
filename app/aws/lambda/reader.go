package lambda

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/pkg/errors"
	"github.com/viant/datly/app/aws/apigw"
	"github.com/viant/datly/app/aws/bridge"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/singleton"
	"os"
)

func StartReader(baseURL string) {
	lambda.Start(handleLambdaReadRequest(baseURL))
}

func handleLambdaReadRequest(baseURL string) func(ctx context.Context, apiRequest apigw.ProxyRequest) (*events.APIGatewayProxyResponse, error) {
	return func(ctx context.Context, apiRequest apigw.ProxyRequest) (*events.APIGatewayProxyResponse, error) {
		response, err := handleReadRequest(ctx, baseURL, apiRequest)
		return &response.APIGatewayProxyResponse, err
	}
}

func handleReadRequest(ctx context.Context, baseURL string, apiRequest apigw.ProxyRequest) (response *apigw.ProxyResponse, err error) {
	apiRequest.Init()
	response = apigw.NewProxyResponse()
	if apiRequest.AsyncMode {
		invocationContext, cancel := context.WithTimeout(ctx, APIGWTimeout)
		defer cancel()
		response, err := runInBackground(invocationContext, apiRequest)
		if err != nil && invocationContext.Err() != nil {
			return handleAPIGWTimeout(baseURL, apiRequest), nil
		}
		return response, err
	}

	readErr := handleDataRead(ctx, apiRequest, response)
	if readErr != nil {
		response = bridge.ErrorAPIGatewayResponse(readErr)
		return
	}
	response, err = handleReadResponse(ctx, apiRequest, response, baseURL)
	return response, err
}

func handleReadResponse(ctx context.Context, apiRequest apigw.ProxyRequest, response *apigw.ProxyResponse, baseURL string) (*apigw.ProxyResponse, error) {
	bridge.SetCORSHeaderIfNeeded(&apiRequest, response)
	compressIfNeeded(response)
	if len(response.Body) > BodyLimit || apiRequest.S3Proxy {
		writeError := proxyResponseWithS3(ctx, apiRequest, baseURL, response)
		if writeError != nil {
			response = bridge.ErrorAPIGatewayResponse(writeError)
			return response, nil
		}
	}
	return response, nil
}
func handleDataRead(ctx context.Context, apiRequest apigw.ProxyRequest, response *apigw.ProxyResponse) error {
	config := os.Getenv(shared.ConfigKey)
	service, err := singleton.Reader(ctx, config)
	if err != nil {
		return errors.Wrapf(err, "failed to create reader with config: %v", config)
	}
	httpRequest, err := bridge.NewHTTPRequest(&apiRequest)
	if err != nil {
		return errors.Wrapf(err, "failed to create http.Request")
	}
	writer := bridge.NewHTTPWriter(response)
	handle := reader.HandleRead(service)
	handle(writer, httpRequest)
	return nil
}
