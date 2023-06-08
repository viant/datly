package session

import (
	"context"
	"github.com/viant/xdatly/handler/differ"
)

type Differ struct {
}

func (d *Differ) Diff(ctx context.Context, from, to interface{}, options ...differ.Option) *differ.ChangeLog {
	return nil
}
