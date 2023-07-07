package command

import (
	"context"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
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
	info, err := plugin.NewInfo(ctx, gen.GoModuleLocation())
	if err != nil {
		return err
	}

	if err := s.generateTemplate(ctx, gen, template, info); err != nil {
		return err
	}

	pkg := info.Package(gen.Package)
	if err = s.generateState(ctx, pkg, gen, template, info); err != nil {
		return err
	}
	if err = s.generateEntity(ctx, pkg, gen, info, template); err != nil {
		return err
	}
	switch info.IntegrationMode {
	case plugin.ModeExtension, plugin.ModeCustomTypeModule:
		if len(info.CustomTypesPackages) == 0 {
			if err := s.tidyModule(ctx, gen.GoModuleLocation()); err != nil {
				return err
			}
		}
		info.UpdateTypesCorePackage(url.Join(gen.GoModuleLocation(), gen.Package))
	default:
		if ok, _ := s.fs.Exists(ctx, url.Join(gen.GoModuleLocation(), "go.mod")); ok {
			if err := s.tidyModule(ctx, gen.GoModuleLocation()); err != nil {
				return err
			}
			info, _ = plugin.NewInfo(ctx, gen.GoModuleLocation())
		}
	}
	return s.EnsurePluginArtifacts(ctx, info)
}

func (s *Service) generateTemplate(ctx context.Context, gen *options.Gen, template *codegen.Template, info *plugin.Info) error {
	//needed for both go and velty
	opts := s.dsqlGenerationOptions(gen)
	files, err := s.generateTemplateFiles(gen, template, info, opts...)
	if err != nil {
		return err
	}
	return s.uploadFiles(ctx, files...)
}

func (s *Service) uploadFiles(ctx context.Context, files ...*File) error {
	for _, f := range files {
		if err := f.validate(); err != nil {
			return err
		}
		if err := s.uploadContent(ctx, f.URL, f.Content); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) uploadContent(ctx context.Context, URL string, content string) error {
	_ = s.fs.Delete(ctx, URL)
	return s.fs.Upload(ctx, URL, file.DefaultFileOsMode, strings.NewReader(content))
}

func (s *Service) dsqlGenerationOptions(gen *options.Gen) []codegen.Option {
	var options []codegen.Option
	if gen.Lang == ast.LangGO {
		options = append(options, codegen.WithoutBusinessLogic())
		options = append(options, codegen.WithLang(gen.Lang))
	}
	return options
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

func (s *Service) generateState(ctx context.Context, pkg string, gen *options.Gen, template *codegen.Template, info *plugin.Info) error {
	code := template.GenerateState(pkg, info)
	return s.fs.Upload(ctx, gen.StateLocation(), file.DefaultFileOsMode, strings.NewReader(code))
}

func (s *Service) ensureDest(ctx context.Context, URL string) error {
	if ok, _ := s.fs.Exists(ctx, URL); ok {
		return nil
	}
	return s.fs.Create(ctx, URL, file.DefaultDirOsMode, true)
}
