package apigw

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/datly/v0/shared"
	"github.com/viant/toolbox"
	"strings"
	"time"
)

const (
	traceIDHeader = "X-Amzn-Trace-Id"
	asyncMode     = "Datly-Async"
	s3Proxy       = "Datly-S3Proxy"
)

//ProxyRequest represents a proxt request
type ProxyRequest struct {
	events.APIGatewayProxyRequest
	Created   time.Time
	TraceID   string
	JobID     string
	AsyncMode bool
	S3Proxy   bool
}

//Init initialises request
func (r *ProxyRequest) Init() {
	r.Created = time.Now()
	if len(r.Headers) == 0 {
		r.Headers = make(map[string]string)
	}
	if len(r.MultiValueHeaders) == 0 {
		r.MultiValueHeaders = make(map[string][]string)
	}
	r.TraceID = r.Headers[traceIDHeader]
	async, ok := r.Headers[asyncMode]
	if ok {
		r.AsyncMode = toolbox.AsBoolean(async)
	}
	s3Proxy, ok := r.Headers[s3Proxy]
	if ok {
		r.S3Proxy = toolbox.AsBoolean(s3Proxy)
	}
	if index := strings.Index(r.TraceID, "-"); index != -1 {
		r.JobID = string(r.TraceID[index+1:])
	}
	r.MultiValueHeaders[shared.EventCreateTimeHeader] = []string{time.Now().Format(time.RFC3339)}
}
