package adapter

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/datly/router/proxy"
	"strings"
)

func NewResponse(writer *proxy.Writer) *events.LambdaFunctionURLResponse {
	response := &events.LambdaFunctionURLResponse{}
	response.Headers = map[string]string{}
	for k, v := range writer.HeaderMap {
		response.Headers[k] = strings.Join(v, ",")
	}
	response.Body = writer.Body.String()
	response.StatusCode = writer.Code
	return response
}
