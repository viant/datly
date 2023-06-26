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
	err := session.Stater().Into(ctx, state)
	fmt.Printf("%v %T %+v\n", err, state, state)
	return &Campaign{Id: 12343}, nil
}
