package command

import (
	"context"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/translator"
	"github.com/viant/datly/view/extension"
)

func (s *Service) ensureTranslator(opts *options.Options) error {
	if s.translator != nil {
		return nil
	}
	aTranslator := translator.New(translator.NewConfig(opts.Repository()), s.fs)
	err := aTranslator.Init(context.Background())
	if err == nil {
		s.translator = aTranslator
	}
	return err
}

func (s *Service) Translate(ctx context.Context, opts *options.Options) (err error) {
	if _, err = s.loadPlugin(ctx, opts); err != nil {
		return err
	}
	if err = s.translate(ctx, opts); err != nil {
		return err
	}
	return s.persistRepository(ctx)
}

func (s *Service) persistRepository(ctx context.Context) error {
	repository := s.translator.Repository
	if err := repository.PersistConfig(); err != nil {
		return err
	}
	if err := repository.Upload(ctx); err != nil {
		return err
	}
	for _, cmd := range s.translator.Plugins {
		if err := s.BuildPlugin(ctx, cmd); err != nil {
			return fmt.Errorf("failed to build plugin: %w", err)
		}
	}
	return nil
}

func (s *Service) translate(ctx context.Context, opts *options.Options) error {
	if err := s.ensureTranslator(opts); err != nil {
		return fmt.Errorf("failed to create translator: %v", err)
	}
	rule := opts.Rule()
	for rule.Index = 0; rule.Index < len(rule.Source); rule.Index++ {
		extension.InitRegistry()
		_ = s.translator.InitSignature(ctx, rule)
		currRule := opts.Rule()
		sourceURL := currRule.SourceURL()
		_, name := url.Split(sourceURL, file.Scheme)
		fmt.Printf("translating %v\n", name)
		dSQL, err := currRule.LoadSource(ctx, s.fs, sourceURL)
		if err != nil {
			return err
		}
		if err = s.translateDSQL(ctx, rule, dSQL, opts); err != nil {
			return err
		}
		repository := s.translator.Repository
		if err = repository.UploadPartialRules(ctx); err != nil {
			return err
		}
	}

	paths := url.Join(s.translator.Repository.Config.RouteURL, "paths.yaml")
	if ok, _ := s.fs.Exists(ctx, paths); ok {
		_ = s.fs.Delete(ctx, paths)
	}

	return nil

}

func (s *Service) translateDSQL(ctx context.Context, rule *options.Rule, dSQL string, opts *options.Options) error {
	dSQL = s.translator.Repository.Substitutes.Replace(dSQL)
	rule.NormalizeComponent(&dSQL)

	if err := s.buildHandlerIfNeeded(rule, &dSQL); err != nil {
		return err
	}

	if err := s.translator.Translate(ctx, rule, dSQL, opts); err != nil {
		err := fmt.Errorf("\nfailed to translate: %v\n", err)
		return err
	}
	return nil
}
