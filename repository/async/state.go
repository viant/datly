package async

import (
	"context"
	"github.com/viant/datly/view"
)

func (s *State) Init(ctx context.Context, res *view.Resource, mainView *view.View) error {

	resource := view.NewResourcelet(res, mainView)
	if s.UserID != nil {
		if err := s.UserID.Init(ctx, resource); err != nil {
			return err
		}
	}
	if s.JobMatchKey != nil {
		if err := s.JobMatchKey.Init(ctx, resource); err != nil {
			return err
		}
	}
	if s.UserEmail != nil {
		if err := s.UserEmail.Init(ctx, resource); err != nil {
			return err
		}
	}
	return nil
}
