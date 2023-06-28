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
	goFormat "go/format"
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
	info.UpdateTypesCorePackage(url.Join(gen.GoModuleLocation(), gen.Package))
	return s.EnsurePluginArtifacts(ctx, info)
}

func (s *Service) generateTemplate(ctx context.Context, gen *options.Gen, template *codegen.Template) error {
	//needed for both go and velty
	opts := s.dsqlGenerationOptions(gen)
	files, err := s.generateTemplateFiles(gen, template, opts...)
	if err != nil {
		return err
	}

	for _, f := range files {
		content := f.Content
		source, err := goFormat.Source([]byte(content))
		if err == nil {
			content = string(source)
		}

		if err = s.uploadContent(ctx, f.URL, content); err != nil {
			return err
		}
	}

	//TODO generate index codee
	//Generate

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

func (s *Service) generateState(ctx context.Context, pkg string, gen *options.Gen, template *codegen.Template) error {
	code := template.GenerateState(pkg)
	return s.fs.Upload(ctx, gen.StateLocation(), file.DefaultFileOsMode, strings.NewReader(code))
}

func (s *Service) ensureDest(ctx context.Context, URL string) error {
	if ok, _ := s.fs.Exists(ctx, URL); ok {
		return nil
	}
	return s.fs.Create(ctx, URL, file.DefaultDirOsMode, true)
}
