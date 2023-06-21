package campaign

import (
	"context"
	"github.com/viant/xdatly/handler"
)

type Handler struct {
}

func (h *Handler) Exec(ctx context.Context, session handler.Session) (interface{}, error) {
	return "302", nil
}
