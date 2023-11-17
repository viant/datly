package command

import (
	"context"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/asset"
	"github.com/viant/datly/internal/codegen"
	"github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/internal/translator"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/service/executor/handler"
	"github.com/viant/datly/view/extension"
	"github.com/viant/tagly/format/text"
	"path"
	"strings"
)

func (s *Service) Generate(ctx context.Context, options *options.Options) error {
	if err := s.generate(ctx, options); err != nil {
		return err
	}
	return nil
}

func (s *Service) generate(ctx context.Context, options *options.Options) error {
	ruleOption := options.Rule()
	if options.Generate.Operation == "get" {
		return s.generateGet(ctx, options)
	}
	ruleOption.Generated = true
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
		ruleOption.Index = i
		rule := resource.Rule
		root := rule.RootViewlet()
		spec := root.Spec
		if spec == nil {
			return fmt.Errorf("view %v tranlsation spec was empty", root.Name)
		}
		root.Spec.Type.Cardinality = resource.Rule.Route.Output.Cardinality
		template := codegen.NewTemplate(resource.Rule, root.Spec)
		root.Spec.Type.Package = ruleOption.Package()
		template.BuildTypeDef(root.Spec, resource.Rule.GetField(), resource.Rule.Doc.Columns)
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
	s.translator.Repository.PersistAssets = false
	options.UpdateTranslate()
	return s.Translate(ctx, options)
}

func (s *Service) generateGet(ctx context.Context, opts *options.Options) (err error) {
	translate := &options.Translate{
		Repository: *opts.Repository(),
		Rule:       *opts.Rule(),
	}
	opts.Translate = translate
	opts.Generate = nil
	if err = s.translate(ctx, opts); err != nil {
		return err
	}
	moduleLocation := translate.Rule.ModuleLocation
	modulePrefix := translate.Rule.ModulePrefix
	sourceURL := translate.Source[0]
	_, sourceName := path.Split(url.Path(sourceURL))
	sourceName = trimExt(sourceName)

	URI := s.translator.Repository.Resource[0].Rule.URI

	if err = s.persistRepository(ctx); err != nil {
		return err
	}
	s.translator.Repository.Files.Reset()
	componentURL := s.translator.Repository.Config.RouteURL
	datlySrv, err := datly.New(ctx, repository.WithComponentURL(componentURL))
	if err != nil {
		return err
	}

	aComponent, err := datlySrv.Component(ctx, URI)
	if err != nil {
		return err
	}
	var embeds = map[string]string{}
	code := aComponent.GenerateOutputCode(true, embeds)
	destURL := path.Join(moduleLocation, modulePrefix, sourceName+".go")
	if err = s.fs.Upload(ctx, destURL, file.DefaultFileOsMode, strings.NewReader(code)); err != nil {
		return err
	}
	return s.persistEmbeds(ctx, moduleLocation, modulePrefix, embeds, aComponent)

}

func (s *Service) persistEmbeds(ctx context.Context, moduleLocation string, modulePrefix string, embeds map[string]string, component *repository.Component) error {
	embedBaseURL := path.Join(path.Join(moduleLocation, modulePrefix, "sql"))
	rootName := component.View.Name
	formatter := text.DetectCaseFormat(rootName)
	rootSQL := path.Join(embedBaseURL, formatter.Format(rootName, text.CaseFormatLowerUnderscore)+".sql")
	if err := s.fs.Upload(ctx, rootSQL, file.DefaultFileOsMode, strings.NewReader(component.View.Template.Source)); err != nil {
		return err
	}

	for k, v := range embeds {
		embedURL := path.Join(embedBaseURL, k)
		v = strings.ReplaceAll(v, `\n`, "\n")
		if err := s.fs.Upload(ctx, embedURL, file.DefaultFileOsMode, strings.NewReader(v)); err != nil {
			return err
		}
	}
	return nil
}

func trimExt(sourceName string) string {
	if index := strings.LastIndex(sourceName, "."); index != -1 {
		sourceName = sourceName[:index]
	}
	return sourceName
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
		info.UpdateDependencies(url.Join(gen.GoModuleLocation(), gen.Package()))

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

func (s *Service) buildHandlerIfNeeded(ruleOptions *options.Rule, dSQL *string) error {
	rule := &translator.Rule{}
	origin := *dSQL
	if err := rule.ExtractSettings(dSQL); err != nil {
		return err
	}
	if rule.Handler == nil {
		*dSQL = origin
		return nil
	}
	state, err := inference.NewState(ruleOptions.GoModuleLocation(), rule.StateType, extension.Config.Types)
	if err != nil {
		return err
	}
	rule.Handler = &handler.Handler{
		HandlerType: rule.HandlerType,
		StateType:   rule.StateType,
	}
	entityParam := state[0]
	entityType := entityParam.Schema.Type()
	if entityType == nil {
		return fmt.Errorf("entity type was empty")
	}
	aType, err := inference.NewType(rule.StateTypePackage(), entityParam.Name, entityType)
	if err != nil {
		return err
	}
	tmpl := codegen.NewTemplate(rule, &inference.Spec{Type: aType})
	tmpl.Imports.AddType(rule.StateType)
	tmpl.Imports.AddType(rule.HandlerType)
	tmpl.EnsureImports(aType)
	tmpl.State = state
	handlerDSQL, err := tmpl.GenerateDSQL(codegen.WithoutBusinessLogic())
	if err != nil {
		return err
	}
	handlerDSQL += fmt.Sprintf("$Nop($%v)", entityParam.Name)
	*dSQL = handlerDSQL
	return nil
}

func (s *Service) generateTemplate(gen *options.Generate, template *codegen.Template, info *plugin.Info) error {
	//needed for both go and velty
	opts := s.translateGenerationOptions(gen, info)
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

func (s *Service) translateGenerationOptions(gen *options.Generate, info *plugin.Info) []codegen.Option {
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
	if columnCase := text.DetectCaseFormat(entityName); columnCase.IsDefined() {
		entityName = columnCase.Format(entityName, text.CaseFormatLowerUnderscore)
	}
	return entityName
}

func (s *Service) ensureDest(ctx context.Context, URL string) error {
	if ok, _ := s.fs.Exists(ctx, URL); ok {
		return nil
	}
	return s.fs.Create(ctx, URL, file.DefaultDirOsMode, true)
}
