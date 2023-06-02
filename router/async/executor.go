package async

import (
	"context"
	"net/http"
)

type Handler interface {
	Handle(ctx context.Context, record *RecordWithHttp, request *http.Request) error
}
