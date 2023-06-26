package campaign

import (
	"context"
	"fmt"
	"github.com/viant/xdatly/handler"
)

type Handler struct {
}

func (h *Handler) Exec(ctx context.Context, session handler.Session) (interface{}, error) {
	state := &State{}
	session.Stater().Into(ctx, state)
	fmt.Printf("%T %+v\n", state, state)
	return &Campaign{Id: 12343}, nil
}
