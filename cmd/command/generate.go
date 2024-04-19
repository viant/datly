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
	"github.com/viant/datly/view/state"
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
	if _, err := s.loadPlugin(ctx, options); err != nil {
		return err
	}
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
		template.SetResource(resource)
		root.Spec.Type.Package = ruleOption.Package()
		template.BuildTypeDef(root.Spec, resource.Rule.GetField(), resource.Rule.Doc.Columns)
		template.Imports.AddType(resource.Rule.Type)
		template.Imports.AddType(resource.Rule.InputType)

		var opts = []codegen.Option{codegen.WithHTTPMethod(gen.HttpMethod()), codegen.WithLang(gen.Lang)}
		template.BuildInput(spec, resource.Rule.GetField(), opts...)

		registry := resource.Resource.TypeRegistry()
		if parameters := resource.OutputState.FilterByKind(state.KindRequestBody); len(parameters) >= 1 {
			parameters[0].Tag += `  typeName:"` + template.Prefix + `"`
			parameters[0].Schema = state.NewSchema(template.BodyType)
			template.BodyParameter = parameters[0]
		}
		template.OutputType, err = resource.OutputState.Parameters().ReflectType(resource.Rule.Package, registry.Lookup)
		if err != nil {
			return err
		}
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
	sources := opts.Rule().Source
	if err = s.translate(ctx, opts); err != nil {
		return err
	}
	if err = s.persistRepository(ctx); err != nil {
		return err
	}

	for i, resource := range s.translator.Repository.Resource {
		moduleLocation := translate.Rule.ModuleLocation
		modulePrefix := translate.Rule.ModulePrefix
		_, sourceName := path.Split(url.Path(sources[i]))
		sourceName = trimExt(sourceName)
		URI := resource.Rule.URI
		if resource.Rule.Route.Method != "GET" {
			URI = resource.Rule.Route.Method + ":" + URI
		}
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
		var namedResources []string

		if repo := opts.Repository(); repo != nil && len(repo.SubstitutesURL) > 0 {
			namedResources = append(namedResources, repo.SubstitutesURL...)
		}
		code := aComponent.GenerateOutputCode(true, !translate.SkipCompDef, embeds, namedResources...)
		destURL := path.Join(moduleLocation, modulePrefix, sourceName+".go")
		if err = s.fs.Upload(ctx, destURL, file.DefaultFileOsMode, strings.NewReader(code)); err != nil {
			return err
		}
		if err = s.persistEmbeds(ctx, moduleLocation, modulePrefix, embeds, aComponent); err != nil {
			return err
		}

		if err = s.translator.Init(ctx); err != nil {
			return err
		}

	}
	return nil
}

func (s *Service) persistEmbeds(ctx context.Context, moduleLocation string, modulePrefix string, embeds map[string]string, component *repository.Component) error {
	rootName := component.View.Name
	formatter := text.DetectCaseFormat(rootName)
	formatter.Format(rootName, text.CaseFormatLowerUnderscore)
	embedBaseURL := path.Join(path.Join(moduleLocation, modulePrefix, formatter.Format(rootName, text.CaseFormatLowerUnderscore)))
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
	embedContent := make(map[string]string)
	inputCode := template.GenerateInput(pkg, info, embedContent)
	for k, v := range embedContent {
		s.Files.Append(asset.NewFile(gen.EmbedLocation(k), v))
	}
	inputURL := gen.InputLocation(template.FilePrefix(), template.FileMethodFragment())

	s.Files.Append(asset.NewFile(inputURL, inputCode))
	outputCode := template.GenerateOutput(pkg, info)
	s.Files.Append(asset.NewFile(gen.OutputLocation(template.FilePrefix(), template.FileMethodFragment()), outputCode))
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

	aState, err := inference.NewState(ruleOptions.SourceCodeLocation(), rule.InputType, extension.Config.Types)
	if err != nil {
		return err
	}
	rule.Handler = &handler.Handler{
		Type:       rule.Type,
		InputType:  rule.InputType,
		OutputType: rule.OutputType,
		Arguments:  rule.HandlerArgs,
	}
	entityParam := aState[0]
	entityType := entityParam.Schema.Type()
	if entityType == nil {
		return fmt.Errorf("entity type was empty")
	}
	aType, err := inference.NewType(rule.StateTypePackage(), entityParam.Name, entityType)
	if err != nil {
		return err
	}
	tmpl := codegen.NewTemplate(rule, &inference.Spec{Type: aType})
	tmpl.SetResource(&translator.Resource{Rule: rule})
	tmpl.Imports.AddType(rule.InputType)
	tmpl.Imports.AddType(rule.Type)
	tmpl.EnsureImports(aType)
	tmpl.State = aState
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
