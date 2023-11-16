package command

import (
	"fmt"
	"github.com/viant/datly/cmd/options"
)

func (s *Service) configureRouter(opts *options.Options) error {
	if err := s.ensureTranslator(opts); err != nil {
		return fmt.Errorf("failed to create translator: %v", err)
	}
	return nil
}
