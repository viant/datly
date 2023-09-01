package session

import (
	"context"
	reader2 "github.com/viant/datly/service/reader"
	"github.com/viant/datly/view"
)

func (s *State) ReadInto(ctx context.Context, dest interface{}, aView *view.View) error {
	if err := s.SetViewState(ctx, aView); err != nil {
		return err
	}
	aReader := reader2.New()
	return aReader.ReadInto(ctx, dest, aView, reader2.WithResourceState(s.state))
}
