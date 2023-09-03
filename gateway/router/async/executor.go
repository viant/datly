package async

import (
	"context"
	handler "github.com/viant/datly/gateway/router/async/handler"
	"net/http"
)

type Handler interface {
	Handle(ctx context.Context, record *handler.RecordWithHttp, request *http.Request) error
}
