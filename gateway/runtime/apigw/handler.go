package apigw

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/datly/gateway/runtime/serverless"
	"net/http"
	"time"

	"github.com/viant/datly/gateway/router/proxy"
	"github.com/viant/datly/gateway/runtime/apigw/adapter"
)

func HandleRequest(ctx context.Context, request *adapter.Request) (*events.APIGatewayProxyResponse, error) {

	writer := proxy.NewWriter()
	if err := HandleHttpRequest(writer, request); err != nil {
		return nil, err
	}

	return adapter.NewResponse(writer), nil
}

func HandleHttpRequest(writer http.ResponseWriter, apiRequest *adapter.Request) error {
	now := time.Now()
	service, err := serverless.GetService()
	if err != nil {
		return err
	}
	httpRequest := apiRequest.Request(service.JWTSigner)
	service.LogInitTimeIfNeeded(now, writer)
	service.ServeHTTP(writer, httpRequest)
	service.LogInitTimeIfNeeded(now, writer)

	return nil
}
