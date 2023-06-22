package command

import (
	"context"
	"github.com/viant/afs/file"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/codegen"
	"github.com/viant/datly/internal/plugin"
	"strings"
)

func (s *Service) Generate(ctx context.Context, gen *options.Gen, template *codegen.Template) error {
	if err := s.ensureDest(ctx, gen.Dest); err != nil {
		return nil
	}
	//TODO adjust if handler option is used

	if err := s.generateDSQL(ctx, gen.DSQLLocation(), template); err != nil {
		return err
	}
	info, err := plugin.NewInfo(ctx, gen.GoModuleLocation())
	if err != nil {
		return err
	}
	pkg := info.Package(gen.Package)
	if err = s.generateState(ctx, pkg, gen, template); err != nil {
		return err
	}
	if err = s.generateEntity(ctx, pkg, gen, info, template); err != nil {
		return err
	}
	return nil
}

func (s *Service) generateEntity(ctx context.Context, pkg string, gen *options.Gen, info *plugin.Info, template *codegen.Template) error {
	code, err := template.GenerateEntity(ctx, pkg, info)
	if err != nil {
		return err
	}
	return s.fs.Upload(ctx, gen.EntityLocation(), file.DefaultFileOsMode, strings.NewReader(code))
}

func (s *Service) generateState(ctx context.Context, pkg string, gen *options.Gen, template *codegen.Template) error {
	code := template.GenerateState(pkg)
	return s.fs.Upload(ctx, gen.StateLocation(), file.DefaultFileOsMode, strings.NewReader(code))
}

func (s *Service) generateDSQL(ctx context.Context, URL string, template *codegen.Template) error {
	DSQL, err := template.GenerateDSQL()
	if err != nil {
		return err
	}
	_ = s.fs.Delete(ctx, URL)
	err = s.fs.Upload(ctx, URL, file.DefaultFileOsMode, strings.NewReader(DSQL))
	return err
}

func (s *Service) ensureDest(ctx context.Context, URL string) error {
	if ok, _ := s.fs.Exists(ctx, URL); ok {
		return nil
	}
	return s.fs.Create(ctx, URL, file.DefaultDirOsMode, true)
}
