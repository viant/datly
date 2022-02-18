package lambda

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/pkg/errors"
	apigw2 "github.com/viant/datly/v0/app/aws/apigw"
	bridge2 "github.com/viant/datly/v0/app/aws/bridge"
	"github.com/viant/datly/v0/reader"
	"github.com/viant/datly/v0/shared"
	"github.com/viant/datly/v0/singleton"
	"os"
)

func StartReader(baseURL string) {
	lambda.Start(handleLambdaReadRequest(baseURL))
}

func handleLambdaReadRequest(baseURL string) func(ctx context.Context, apiRequest apigw2.ProxyRequest) (*events.APIGatewayProxyResponse, error) {
	return func(ctx context.Context, apiRequest apigw2.ProxyRequest) (*events.APIGatewayProxyResponse, error) {
		response, err := handleReadRequest(ctx, baseURL, apiRequest)
		return &response.APIGatewayProxyResponse, err
	}
}

func handleReadRequest(ctx context.Context, baseURL string, apiRequest apigw2.ProxyRequest) (response *apigw2.ProxyResponse, err error) {
	apiRequest.Init()
	response = apigw2.NewProxyResponse()
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
		response = bridge2.ErrorAPIGatewayResponse(readErr)
		return
	}
	response, err = handleReadResponse(ctx, apiRequest, response, baseURL)
	return response, err
}

func handleReadResponse(ctx context.Context, apiRequest apigw2.ProxyRequest, response *apigw2.ProxyResponse, baseURL string) (*apigw2.ProxyResponse, error) {
	bridge2.SetCORSHeaderIfNeeded(&apiRequest, response)
	compressIfNeeded(response)
	if len(response.Body) > BodyLimit || apiRequest.S3Proxy {
		writeError := proxyResponseWithS3(ctx, apiRequest, baseURL, response)
		if writeError != nil {
			response = bridge2.ErrorAPIGatewayResponse(writeError)
			return response, nil
		}
	}
	return response, nil
}
func handleDataRead(ctx context.Context, apiRequest apigw2.ProxyRequest, response *apigw2.ProxyResponse) error {
	config := os.Getenv(shared.ConfigKey)
	service, err := singleton.Reader(ctx, config)
	if err != nil {
		return errors.Wrapf(err, "failed to create reader with config: %v", config)
	}
	httpRequest, err := bridge2.NewHTTPRequest(&apiRequest)
	if err != nil {
		return errors.Wrapf(err, "failed to create http.Request")
	}
	writer := bridge2.NewHTTPWriter(response)
	handle := reader.HandleRead(service)
	handle(writer, httpRequest)
	return nil
}
