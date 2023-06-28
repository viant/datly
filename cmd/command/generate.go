package command

import (
	"context"
	"github.com/viant/afs/file"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/codegen"
	"github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/toolbox/format"
	"strings"
)

func (s *Service) Generate(ctx context.Context, gen *options.Gen, template *codegen.Template) error {
	if err := s.ensureDest(ctx, gen.Dest); err != nil {
		return err
	}
	//TODO adjust if handler option is used
	if err := s.generateTemplate(ctx, gen, template); err != nil {
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

func (s *Service) generateTemplate(ctx context.Context, gen *options.Gen, template *codegen.Template) error {
	URL := gen.DSQLLocation()
	templateContent, err := s.generateTemplateContent(ctx, gen, template, URL)
	if err != nil {
		return err
	}

	_ = s.fs.Delete(ctx, URL)
	err = s.fs.Upload(ctx, URL, file.DefaultFileOsMode, strings.NewReader(templateContent))

	return nil
}

func (s *Service) generateTemplateContent(ctx context.Context, gen *options.Gen, template *codegen.Template, URL string) (string, error) {
	switch gen.Lang {
	case ast.LangGO:
		return template.GenerateGo(gen)
	default:
		return s.generateDSQL(ctx, URL, template)
	}
}

func (s *Service) generateEntity(ctx context.Context, pkg string, gen *options.Gen, info *plugin.Info, template *codegen.Template) error {
	code, err := template.GenerateEntity(ctx, pkg, info)
	if err != nil {
		return err
	}
	entityName := ensureGoFileCaseFormat(template)

	return s.fs.Upload(ctx, gen.EntityLocation(entityName), file.DefaultFileOsMode, strings.NewReader(code))
}

func ensureGoFileCaseFormat(template *codegen.Template) string {
	entityName := template.Spec.Type.Name
	if columnCase, err := format.NewCase(formatter.DetectCase(entityName)); err == nil {
		entityName = columnCase.Format(entityName, format.CaseLowerUnderscore)
	}
	return entityName
}

func (s *Service) generateState(ctx context.Context, pkg string, gen *options.Gen, template *codegen.Template) error {
	code := template.GenerateState(pkg)
	return s.fs.Upload(ctx, gen.StateLocation(), file.DefaultFileOsMode, strings.NewReader(code))
}

func (s *Service) generateDSQL(ctx context.Context, URL string, template *codegen.Template) (string, error) {
	DSQL, err := template.GenerateDSQL()
	return DSQL, err
}

func (s *Service) ensureDest(ctx context.Context, URL string) error {
	if ok, _ := s.fs.Exists(ctx, URL); ok {
		return nil
	}
	return s.fs.Create(ctx, URL, file.DefaultDirOsMode, true)
}
