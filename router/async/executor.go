package async

import (
	"context"
	handler2 "github.com/viant/datly/router/async/handler"
	"net/http"
)

type Handler interface {
	Handle(ctx context.Context, record *handler2.RecordWithHttp, request *http.Request) error
}
