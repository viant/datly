package session

import (
	"context"
	reader "github.com/viant/datly/service/reader"
	"github.com/viant/datly/view"
)

func (s *Session) ReadInto(ctx context.Context, dest interface{}, aView *view.View) error {
	if err := s.SetViewState(ctx, aView); err != nil {
		return err
	}
	aReader := reader.New()
	return aReader.ReadInto(ctx, dest, aView, reader.WithResourceState(s.state))
}
