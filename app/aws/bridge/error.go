package bridge

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/datly/app/aws/apigw"
	"net/http"
)

//ErrorAPIGatewayResponse returns error api gateway response
func ErrorAPIGatewayResponse(err error) *apigw.ProxyResponse {
	return &apigw.ProxyResponse{
		APIGatewayProxyResponse: events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       err.Error(),
		}}
}
