package command

import (
	"context"
	"fmt"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/translator"
)

func (s *Service) ensureTranslator(opts *options.Options) error {
	if s.translator != nil {
		return nil
	}
	aTranslator := translator.New(translator.NewConfig(opts.Repository()))
	err := aTranslator.Init(context.Background())
	if err == nil {
		s.translator = aTranslator
	}
	return err
}

func (s *Service) Translate(ctx context.Context, opts *options.Options) error {
	err := s.translate(ctx, opts)
	if err != nil {
		return err
	}
	repository := s.translator.Repository
	if err := repository.PersistConfig(); err != nil {
		return err
	}
	return repository.Upload(ctx)
}

func (s *Service) translate(ctx context.Context, opts *options.Options) error {
	if err := s.ensureTranslator(opts); err != nil {
		return fmt.Errorf("failed to create translator: %v", err)
	}
	rule := opts.Rule()
	for rule.Index = 0; rule.Index < len(rule.Source); rule.Index++ {
		dSQL, err := opts.Rule().LoadSource(ctx, s.fs)
		if err != nil {
			return err
		}
		if err = s.translateDSQL(ctx, rule, dSQL); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) translateDSQL(ctx context.Context, rule *options.Rule, dSQL string) error {
	if err := s.translator.Translate(ctx, rule, dSQL); err != nil {
		fmt.Printf("failed to translate: %v: %w", err)
	}
	return nil
}
