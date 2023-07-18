package command

import (
	"context"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/asset"
	"github.com/viant/datly/internal/codegen"
	"github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/toolbox/format"
	"strings"
)

func (s *Service) Generate(ctx context.Context, options *options.Options) error {
	if err := s.generate(ctx, options); err != nil {
		return err
	}
	return nil
}

func (s *Service) generate(ctx context.Context, options *options.Options) error {
	rule := options.Rule()
	rule.Generated = true
	if err := s.translate(ctx, options); err != nil {
		return err
	}
	s.translator.Repository.Files.Reset()
	gen := options.Generate
	goModule := gen.GoModuleLocation()
	info, err := plugin.NewInfo(ctx, goModule)
	if err != nil {
		return err
	}
	for i, resource := range s.translator.Repository.Resource {
		rule.Index = i
		root := resource.Rule.RootViewlet()
		spec := root.Spec
		//		bodyParams := resource.State.FilterByKind(view.KindRequestBody)
		root.Spec.Type.Cardinality = resource.Rule.Cardinality
		if resource.Rule.ShallGenerateHandler() {

			/*

				statePackage := builder.option.StatePackage()
				state, err := inference.NewState(statePath, builder.option.StateType, config.Config.Types)
				if err != nil {
					return "", err
				}
				entityParam := state[0]
				entityType := entityParam.Schema.Type()
				if entityType == nil {
					return "", fmt.Errorf("entity type was empty")
				}
				aType, err := inference.NewType(statePackage, entityParam.Name, entityType)

				if err != nil {
					return "", err
				}
				tmpl := codegen.NewTemplate(builder.option, &inference.Spec{Type: aType})
				//if entityParam.In.Kind == view.KindRequestBody {
				//	if entityParam.In.Name != "" {
				//		tmpl.Imports.AddType(aType.Name)
				//	}
				//}
				tmpl.Imports.AddType(builder.option.StateType)
				tmpl.Imports.AddType(builder.option.HandlerType)

				tmpl.EnsureImports(aType)
				tmpl.State = state

				if builder.option.Declare == nil {
					builder.option.Declare = map[string]string{}
				}

				//builder.option.Declare["Handler"] = simpledName(builder.option.HandlerType)
				//builder.option.Declare["State"] = simpledName(builder.option.StateType)

				//builder.option.TypeSrc = &option.TypeSrcConfig{}
				//builder.option.TypeSrc.URL = path.Join(statePath, statePackage)
				//builder.option.TypeSrc.Types = append(builder.option.TypeSrc.Types, builder.option.HandlerType, builder.option.StateType)

				dSQL, err := tmpl.GenerateDSQL(codegen.WithoutBusinessLogic())

			*/
		}

		template := codegen.NewTemplate(resource.Rule, root.Spec)
		root.Spec.Type.Package = rule.Package()
		template.BuildTypeDef(root.Spec, resource.Rule.GetField())
		template.Imports.AddType(resource.Rule.HandlerType)
		template.Imports.AddType(resource.Rule.StateType)

		var opts = []codegen.Option{codegen.WithHTTPMethod(gen.HttpMethod()), codegen.WithLang(gen.Lang)}
		template.BuildState(spec, resource.Rule.GetField(), opts...)
		template.BuildLogic(spec, opts...)
		if err := s.generateCode(ctx, options.Generate, template, info); err != nil {
			return err
		}
	}

	if err := s.Files.Upload(ctx, s.fs); err != nil {
		return err
	}
	info, err = plugin.NewInfo(ctx, gen.GoModuleLocation())
	if err != nil {
		return err
	}
	if err = s.updateModule(ctx, gen, info); err != nil {
		return err
	}
	s.translator.Repository.Resource = nil
	options.UpdateTranslate()
	return s.Translate(ctx, options)
}

func (s *Service) generateCode(ctx context.Context, gen *options.Generate, template *codegen.Template, info *plugin.Info) error {
	pkg := info.Package(gen.Package())

	//TODO adjust if handler option is used
	if err := s.generateTemplate(gen, template, info); err != nil {
		return err
	}
	code := template.GenerateState(pkg, info)
	s.Files.Append(asset.NewFile(gen.StateLocation(), code))
	return s.generateEntity(ctx, pkg, gen, info, template)
}

func (s *Service) updateModule(ctx context.Context, gen *options.Generate, info *plugin.Info) error {
	switch info.IntegrationMode {
	case plugin.ModeExtension, plugin.ModeCustomTypeModule:
		if len(info.CustomTypesPackages) == 0 {
			if err := s.tidyModule(ctx, gen.GoModuleLocation()); err != nil {
				return err
			}
		}
		info.UpdateTypesCorePackage(url.Join(gen.GoModuleLocation(), gen.Package()))
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

func (s *Service) generateTemplate(gen *options.Generate, template *codegen.Template, info *plugin.Info) error {
	//needed for both go and velty
	opts := s.dsqlGenerationOptions(gen, info)
	return s.generateTemplateFiles(gen, template, info, opts...)
}

func (s *Service) uploadFiles(ctx context.Context, files ...*asset.File) error {
	for _, f := range files {
		if err := f.Validate(); err != nil {
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

func (s *Service) dsqlGenerationOptions(gen *options.Generate, info *plugin.Info) []codegen.Option {
	var options []codegen.Option
	if gen.Lang == ast.LangGO {
		options = append(options, codegen.WithoutBusinessLogic())
		options = append(options, codegen.WithLang(gen.Lang))
	}

	return options
}

func (s *Service) generateEntity(ctx context.Context, pkg string, gen *options.Generate, info *plugin.Info, template *codegen.Template) error {
	code, err := template.GenerateEntity(ctx, pkg, info)
	if err != nil {
		return err
	}
	entityName := ensureGoFileCaseFormat(template)
	s.Files.Append(asset.NewFile(gen.EntityLocation(entityName), code))
	return nil
}

func ensureGoFileCaseFormat(template *codegen.Template) string {
	entityName := template.Spec.Type.Name
	if columnCase, err := format.NewCase(formatter.DetectCase(entityName)); err == nil {
		entityName = columnCase.Format(entityName, format.CaseLowerUnderscore)
	}
	return entityName
}

func (s *Service) ensureDest(ctx context.Context, URL string) error {
	if ok, _ := s.fs.Exists(ctx, URL); ok {
		return nil
	}
	return s.fs.Create(ctx, URL, file.DefaultDirOsMode, true)
}
