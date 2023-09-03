package adapter

import (
	"encoding/base64"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/afs/option/content"
	"github.com/viant/datly/gateway/router/proxy"
	"github.com/viant/datly/utils/httputils"
	"strconv"
	"strings"
)

func NewResponse(writer *proxy.Writer) *events.APIGatewayProxyResponse {
	response := &events.APIGatewayProxyResponse{}
	response.Headers = map[string]string{}
	for k, v := range writer.HeaderMap {
		response.Headers[k] = strings.Join(v, ",")
	}
	if enc, ok := response.Headers[content.Encoding]; ok && enc == httputils.EncodingGzip {
		response.Body = base64.StdEncoding.EncodeToString(writer.Body.Bytes())
		response.IsBase64Encoded = true
		response.Headers[httputils.ContentLength] = strconv.Itoa(len(response.Body))
	} else {
		response.Body = writer.Body.String()
	}
	response.StatusCode = writer.Code
	return response
}
