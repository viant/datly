package adapter

import (
	"encoding/base64"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/afs/option/content"
	"github.com/viant/datly/router"
	"github.com/viant/datly/router/proxy"
	"strconv"
	"strings"
)

func NewResponse(writer *proxy.Writer) *events.LambdaFunctionURLResponse {
	response := &events.LambdaFunctionURLResponse{}
	response.Headers = map[string]string{}
	for k, v := range writer.HeaderMap {
		response.Headers[k] = strings.Join(v, ",")
	}
	if enc, ok := response.Headers[content.Encoding]; ok && enc == router.EncodingGzip {
		response.Body = base64.StdEncoding.EncodeToString(writer.Body.Bytes())
		response.IsBase64Encoded = true
		response.Headers[router.ContentLength] = strconv.Itoa(len(response.Body))
	} else {
		response.Body = writer.Body.String()
	}
	response.StatusCode = writer.Code
	return response
}
