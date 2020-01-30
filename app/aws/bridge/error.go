package bridge

import (
	"github.com/aws/aws-lambda-go/events"
	"net/http"
)

//ErrorAPIGatewayResponse returns error api gateway response
func ErrorAPIGatewayResponse(err error) events.APIGatewayProxyResponse {
	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusInternalServerError,
		Body:       err.Error(),
	}
}
