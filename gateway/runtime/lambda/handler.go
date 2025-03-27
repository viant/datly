package lambda

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/datly/gateway/router/proxy"
	"github.com/viant/datly/gateway/runtime/lambda/adapter"
	"github.com/viant/datly/gateway/runtime/serverless"
	"net/http"
	"time"
)

func HandleRequest(ctx context.Context, request *adapter.Request) (*events.LambdaFunctionURLResponse, error) {
	httpRequest := request.Request()
	writer := proxy.NewWriter()
	if err := HandleHttpRequest(writer, httpRequest); err != nil {
		return nil, err
	}
	return adapter.NewResponse(writer), nil
}

func HandleHttpRequest(writer http.ResponseWriter, httpRequest *http.Request) error {
	service, err := serverless.GetService()
	if err != nil {
		return err
	}
	service.ServeHTTP(writer, httpRequest)
	return nil
}
