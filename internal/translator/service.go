package translator

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/internal/asset"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/internal/translator/parser"
	signature "github.com/viant/datly/repository/contract/signature"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/service"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/discover"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xreflect"
	"golang.org/x/mod/modfile"
	"gopkg.in/yaml.v3"
	"net/http"
	spath "path"
	"reflect"
	"strings"
	"time"
)

type Service struct {
	Repository *Repository //TODO init repo with basic config and dependencies
	Plugins    []*options.Plugin
	fs         afs.Service
	signature  *signature.Service
}

func (s *Service) InitSignature(ctx context.Context, rule *options.Rule) (err error) {
	prefix := spath.Join(s.Repository.Config.APIPrefix, rule.ModulePrefix)
	if s.signature, err = signature.New(ctx, prefix, s.Repository.Config.RouteURL); err != nil {
		return err
	}
	return nil
}

func (s *Service) Translate(ctx context.Context, rule *options.Rule, dSQL string, opts *options.Options) (err error) {
	resource := NewResource(rule, s.Repository.Config.repository, &s.Repository.Messages)
	s.detectModule(ctx, rule, resource)

	resource.Resource.Substitutes = s.Repository.Substitutes.Merge()
	//resource.State.Append(s.Repository.State...)
	if err = resource.InitRule(&dSQL, ctx, s.Repository.fs, opts); err != nil {
		return err
	}
	if err = resource.IncludeSnippets(ctx, s.fs, &dSQL); err != nil {
		return err
	}
	if err = resource.parseImports(ctx, &dSQL); err != nil {
		return err
	}
	if resource.Rule.ContentURL != "" {
		return s.buildStaticContent(ctx, rule, resource, opts)
	}
	if err = resource.ExtractDeclared(&dSQL); err != nil {
		return err
	}
	for _, candidate := range resource.State.FilterByKind(state.KindConst) {
		if param := s.Repository.State.Lookup(candidate.Name); param != nil {
			candidate.Value = param.Value
		}
	}

	if err = s.updateComponentType(ctx, resource, resource.State.FilterByKind(state.KindComponent)); err != nil {
		return err
	}
	if err = s.updateComponentType(ctx, resource, resource.OutputState.FilterByKind(state.KindComponent)); err != nil {
		return err
	}

	dSQL = rule.NormalizeSQL(dSQL, parser.OnVeltyExpression)
	if resource.IsExec() || resource.Rule.Handler != nil {
		if err := s.translateExecutorDSQL(ctx, resource, dSQL); err != nil {
			return err
		}
	} else {
		if err = s.translateReaderDSQL(ctx, resource, dSQL); err != nil {
			return err
		}
	}
	s.Repository.Resource = append(s.Repository.Resource, resource)
	s.Repository.PersistAssets = true
	return nil
}

func (s *Service) detectModule(ctx context.Context, rule *options.Rule, resource *Resource) {
	modFile := url.Join(rule.ModuleLocation, "go.mod")
	if ok, _ := s.fs.Exists(ctx, modFile); ok {
		data, _ := s.fs.DownloadWithURL(ctx, modFile)
		if goMod, _ := modfile.Parse(modFile, data, nil); goMod != nil {
			resource.Module = goMod.Module
			resource.ModuleLocation = rule.ModuleLocation
		}
	}
}

func (s *Service) discoverComponentContract(ctx context.Context, resource *Resource, location *state.Location) (*signature.Signature, error) {
	var err error
	if s.signature == nil {
		if err = s.InitSignature(ctx, resource.rule); err != nil {
			return nil, err
		}
	}
	location.Name = strings.ReplaceAll(location.Name, "..", "[]")
	location.Name = strings.ReplaceAll(location.Name, ".", "/")
	method, URI := shared.ExtractPath(location.Name)
	return s.signature.Signature(method, URI)
}

func (s *Service) translateExecutorDSQL(ctx context.Context, resource *Resource, DSQL string) (err error) {
	if err = s.buildExecutorView(ctx, resource, DSQL); err != nil {
		return err
	}
	resource.buildParameterViews()
	if err := resource.ensureViewParametersSchema(ctx, s.buildQueryViewletType); err != nil {
		return err
	}
	if err := resource.ensurePathParametersSchema(ctx, resource.State); err != nil {
		return err
	}

	if err = resource.Rule.Viewlets.Each(func(viewlet *Viewlet) error {
		return s.adjustView(viewlet, resource, view.ModeExec)
	}); err != nil {
		return err
	}

	route := &resource.Rule.Route
	s.ensureExcutorContract(route, resource)
	if err = s.persistRouterRule(ctx, resource, service.TypeExecutor); err != nil {
		return err
	}
	return nil
}

func (s *Service) ensureExcutorContract(route *router.Route, resource *Resource) {
	if route.Handler != nil {
		return
	}
	root := resource.Resource.Parameters

	fmt.Println("root", root)

	if route.Handler == nil && len(resource.OutputState) > 0 && len(route.Component.Output.Type.Parameters) == 0 {
		for _, parameter := range resource.OutputState {
			if parameter.In.Kind == state.KindRequestBody {
				if body := resource.State.FilterByKind(parameter.In.Kind); len(body) > 0 {
					parameter.Schema = body[0].Schema
				}
			}
		}
		route.Component.Output.Type.Parameters = resource.OutputState.Parameters()
	}
}

func (s *Service) buildExecutorView(ctx context.Context, resource *Resource, DSQL string) (err error) {
	ruleName := s.Repository.RuleName(resource.rule)
	resource.Rule.Root = ruleName
	viewlet := NewViewlet(ruleName, DSQL, nil, resource)
	viewlet.Table, err = s.extractDMLTables(ctx, resource)
	if err != nil {
		return fmt.Errorf("failed to build exec view: %v, unable to extract DML tables: %w", ruleName, err)
	}
	if viewlet.Table != nil {
		viewlet.View.TableBatches = map[string]bool{
			viewlet.Table.Name: true,
		}
	}
	viewlet.Connector = s.DefaultConnector()
	resource.Rule.Viewlets.Append(viewlet)
	SQL := viewlet.Resource.State.Expand(viewlet.SQL)
	aTemplate, err := parser.NewTemplate(viewlet.SQL, &viewlet.Resource.State)
	if err != nil {
		return fmt.Errorf("invalid DSQL: %w, %s", err, SQL)
	}
	aTemplate.DetectTypes(viewlet.UpdateParameterType)
	viewlet.SanitizedSQL = aTemplate.Sanitize()
	return err
}

func (s *Service) translateReaderDSQL(ctx context.Context, resource *Resource, dSQL string) error {
	aQuery, err := sqlparser.ParseQuery(dSQL, parser.OnVeltyExpression())
	if err != nil {
		return err
	}
	resource.Rule.Root = aQuery.From.Alias
	if err = s.updateCodecParameters(ctx, resource); err != nil {
		return err
	}
	if err = resource.Rule.Viewlets.Init(ctx, aQuery, resource, s.initReaderViewlet, s.buildQueryViewletType); err != nil {
		return err
	}

	root := resource.Rule.RootView()
	root.Module = resource.rule.ModulePrefix
	if err = root.buildView(resource.Rule, view.ModeQuery); err != nil {
		return err
	}
	rootViewlet := resource.Rule.RootViewlet()

	if err = s.updateExplicitInputType(resource, resource.Rule.RootViewlet()); err != nil {
		return err
	}
	componentColumns := discover.Columns{Items: make(map[string]view.Columns)}
	if err = s.detectColumns(resource, componentColumns); err != nil {
		return err
	}
	s.detectComponentViewType(componentColumns, resource)

	if err = s.updateOutputParameters(resource, rootViewlet); err != nil {
		return err
	}
	if err = s.updateExplicitOutputType(resource, resource.Rule.RootViewlet(), resource.OutputState.Parameters()); err != nil {
		return err
	}
	if viewParameter := resource.OutputState.Parameters().LookupByLocation(state.KindOutput, "view"); viewParameter != nil && !viewParameter.IsAnonymous() {
		rootViewlet.Holder = viewParameter.Name
	}
	if err = resource.Rule.updateExclude(rootViewlet); err != nil {
		return err
	}

	if err = resource.Rule.Viewlets.Each(func(viewlet *Viewlet) error {
		return s.adjustView(viewlet, resource, view.ModeQuery)
	}); err != nil {
		return err
	}
	if err = s.persistRouterRule(ctx, resource, service.TypeReader); err != nil {
		return err
	}
	return nil
}

func (s *Service) updateViewOutputType(viewlet *Viewlet, withTypeDef bool, documentation state.Documentation) {
	if viewlet.Resource.Rule.IsGeneratation {
		return
	}

	if schema := viewlet.View.Schema; schema != nil && (schema.Type() != nil || (schema.DataType != "" && withTypeDef)) {
		return
	}

	for _, relField := range viewlet.Spec.Type.RelationFields {
		relViewlet := viewlet.Resource.Rule.Viewlets.Lookup(relField.Relation)
		s.updateViewOutputType(relViewlet, false, documentation)

		relType := relViewlet.View.Schema.Type()
		if relType.Kind() == reflect.Struct {
			relType = reflect.PointerTo(relType)
		}
		if relField.Cardinality == state.Many {
			relType = reflect.SliceOf(relType)
		}
		relField.Schema = state.NewSchema(relType)
		relField.Schema.Cardinality = relField.Cardinality
		relField.Name = relViewlet.Holder
	}

	fields := viewlet.Spec.Type.Fields()
	viewlet.Spec.Type.Fields()

	if len(fields) > 0 {
		if viewlet.View.Schema == nil {
			viewlet.View.Schema = &state.Schema{}
		}
		paramSchema := reflect.StructOf(fields)
		if viewlet.View.Schema.Cardinality == "" {
			viewlet.View.Schema.Cardinality = state.Many
		}
		viewlet.View.Schema.SetType(paramSchema)
	}
	if !withTypeDef {
		return
	}

	viewlet.TypeDefinition = viewlet.Spec.TypeDefinition("", false, documentation)
	viewlet.TypeDefinition.Cardinality = ""
	viewlet.TypeDefinition.Name = view.DefaultTypeName(viewlet.Name)
}

func (s *Service) persistViewMetaColumn(cache discover.Columns, resource *Resource) error {
	if len(cache.Items) == 0 {
		return nil
	}
	cache.ModTime = time.Now()
	data, err := yaml.Marshal(cache)
	if err != nil {
		return fmt.Errorf("failed to encode: %T, %w", cache, err)
	}
	baseRuleURL := s.Repository.RuleBaseURL(resource.rule)
	ruleName := s.Repository.RuleName(resource.rule)
	cacheURL := url.Join(baseRuleURL, ".meta", ruleName+".yaml")
	s.Repository.Files.Append(asset.NewFile(cacheURL, string(data)))
	return nil
}

func (s *Service) persistRouterRule(ctx context.Context, resource *Resource, serviceType service.Type) error {
	baseRuleURL := s.Repository.RuleBaseURL(resource.rule)
	route := &resource.Rule.Route
	ruleName := s.Repository.RuleName(resource.rule)
	route.Service = serviceType
	route.View = view.NewRefView(resource.Rule.Root)
	if resource.Module != nil {
		route.Contract.ModulePath = resource.Module.Mod.Path
	}

	if route.Handler != nil {
		if route.Component.Output.Type.Schema == nil {
			route.Component.Output.Type.Schema = &state.Schema{}
		}
		if route.Component.Input.Type.Schema == nil {
			route.Component.Input.Type.Schema = &state.Schema{}
		}
		route.Component.Output.Type.Package,
			route.Component.Output.Type.Name = extractTypeNameWithPackage(route.Handler.OutputType)

		route.Component.Input.Type.Package,
			route.Component.Input.Type.Name = extractTypeNameWithPackage(route.Handler.InputType)

		if len(resource.State) > 0 && route.Component.Input.Type.Name == "" {
			route.Component.Input.Type.Parameters = resource.State.Parameters()
			res := &view.Resource{}
			res.SetTypes(resource.typeRegistry)
			route.Component.Input.Type.Init(state.WithResource(view.NewResources(res, &view.View{})))
			formatter := text.DetectCaseFormat(ruleName)
			inputName := formatter.To(text.CaseFormatUpperCamel).Format(ruleName) + "Input"
			route.Component.Input.Type.Name = inputName
			route.Handler.InputType = inputName
			resource.AppendTypeDefinition(&view.TypeDefinition{Name: inputName, DataType: route.Component.Input.Type.Schema.Type().String()})
		}
	}

	route.Content.CSV = resource.Rule.CSV
	route.Content.TabularJSON = resource.Rule.TabularJSON
	route.Content.XML = resource.Rule.XML
	if resource.Rule.XMLUnmarshalType != "" {
		route.Content.Marshaller.XML.TypeName = resource.Rule.XMLUnmarshalType
	}
	if resource.Rule.JSONUnmarshalType != "" {
		route.Content.Marshaller.JSON.TypeName = resource.Rule.JSONUnmarshalType
	}
	route.Component.Output.DataFormat = resource.Rule.DataFormat

	if err := s.applyAsyncOption(resource, route); err != nil {
		return err
	}

	if resource.rule.Generated { //translation from generator
		resource.Rule.applyGeneratorOutputSetting()

	}
	if !strings.HasPrefix(resource.Rule.Route.URI, resource.repository.APIPrefix) {
		resource.Rule.Route.URI = spath.Join(resource.repository.APIPrefix, resource.rule.ModulePrefix, resource.Rule.Route.URI)
	}
	if resource.Rule.Viewlets.compressionSizeKb > 0 {
		resource.Rule.Compression = &path.Compression{
			MinSizeKb: resource.Rule.Viewlets.compressionSizeKb,
		}
	}
	aState, err := resource.State.Compact(resource.rule.ModuleLocation, resource.typeRegistry)
	if err != nil {
		return fmt.Errorf("failed to compact aState: %w", err)
	}
	resource.Resource.Parameters = aState.RemoveReserved().Parameters()
	rootViewName := ""
	if rootView := resource.Rule.RootView(); rootView != nil {
		rootViewName = rootView.Name
	}

	routerResource, err := s.buildRouterResource(ctx, resource)
	if err != nil {
		return fmt.Errorf("failed to build component : %s, %w", rootViewName, err)
	}

	err = s.persistDocumentation(ctx, resource, routerResource)
	if err != nil {
		return err
	}

	routerResource.Resource.Substitutes = nil
	data, err := asset.EncodeYAML(routerResource)
	if err != nil {
		return fmt.Errorf("failed to encode: %+v, %w", routerResource, err)
	}
	ruleSource := string(data)
	ruleSource = s.Repository.Substitutes.ReverseReplace(ruleSource)
	s.Repository.Files.Append(asset.NewFile(url.Join(baseRuleURL, ruleName+".yaml"), ruleSource))
	return nil
}

func (s *Service) persistDocumentation(ctx context.Context, resource *Resource, routerResource *router.Resource) error {
	if len(resource.Rule.DocURLs) > 0 {
		routerResource.Resource.Docs = &view.Documentation{
			BaseURL: "doc",
		}
		baseDocURL := s.Repository.DocBaseURL()

		for i, docURL := range resource.Rule.DocURLs {
			if strings.HasPrefix(docURL, "./") {
				docURL = docURL[2:]
			}
			destURL := url.Join(baseDocURL, docURL)
			_ = fs.Delete(ctx, destURL)
			if err := fs.Copy(ctx, resource.Rule.Doc.URLs[i], destURL); err != nil {
				return fmt.Errorf("failed to copy doc: %v, %w", resource.Rule.Doc.URLs[i], err)
			}
			routerResource.Resource.Docs.URLs = append(routerResource.Resource.Docs.URLs, docURL)
		}
	}
	return nil
}

func extractTypeNameWithPackage(outputName string) (string, string) {
	if index := strings.Index(outputName, "."); index != -1 {
		return outputName[:index], outputName[index+1:]
	}
	return outputName, ""
}

func (s *Service) adjustView(viewlet *Viewlet, resource *Resource, mode view.Mode) error {

	viewlet.applyOutputShorthands()
	if viewlet.IsMetaView() {
		return nil
	}
	if viewlet.TypeDefinition != nil {
		viewlet.TypeDefinition.Cardinality = ""
	}
	if viewlet.TypeDefinition != nil && viewlet.DataType != "" { //if derived from
		viewlet.TypeDefinition.DataType = strings.ReplaceAll(viewlet.DataType, "*", "")
		viewlet.TypeDefinition.Name = viewlet.TypeDefinition.DataType
		pkgLocation := viewlet.Resource.rule.ComponentPath()
		aType := xreflect.NewType(viewlet.TypeDefinition.Name, xreflect.WithPackage(viewlet.TypeDefinition.SimplePackage()), xreflect.WithPackagePath(pkgLocation))
		rType, _ := aType.LoadType(resource.typeRegistry)
		if rType != nil {
			viewlet.TypeDefinition.DataType = rType.String()
			viewlet.TypeDefinition.Fields = nil
			resource.AppendTypeDefinition(viewlet.TypeDefinition)
		}
	}
	baseRuleURL := s.Repository.RuleBaseURL(resource.rule)
	ruleName := s.Repository.RuleName(resource.rule)
	if err := viewlet.View.buildView(resource.Rule, mode); err != nil {
		return err
	}

	if len(resource.Declarations.QuerySelectors) > 0 {
		for key, state := range resource.Declarations.QuerySelectors {
			return fmt.Errorf("unknown query selector view %v, %v", key, state[0].Name)
		}
	}

	//TODO move cache to dependency but allow local different TTL override
	//	aView := &viewlet.View.View
	//if aView.Columns != nil {
	//	s.GitRepository.AppendCache(aView.Columns)
	//}

	aView := &viewlet.View.View

	resource.Resource.Views = append(resource.Resource.Views, aView)
	viewlet.View.GenerateFiles(baseRuleURL, ruleName, &s.Repository.Files, s.Repository.Substitutes.Merge())
	if viewlet.TypeDefinition != nil {
		if len(viewlet.TypeDefinition.Fields) > 0 {
			viewType := reflect.StructOf(viewlet.Spec.Type.Fields())
			viewlet.TypeDefinition.DataType = viewType.String()
			viewlet.TypeDefinition.Fields = nil
		}
		resource.AppendTypeDefinition(viewlet.TypeDefinition)
	}

	return nil
}

// initReaderViewlet detect SQL dependent Table columns with implicit parameters type to produce sanitized SQL
func (s *Service) initReaderViewlet(ctx context.Context, viewlet *Viewlet) error {

	connector := viewlet.GetConnector()
	if connector == "" {
		connector = s.DefaultConnector()
	}
	viewlet.Connector = connector

	db, err := s.Repository.LookupDb(connector)
	if err != nil {
		return err
	}
	SQL := viewlet.Resource.State.Expand(viewlet.SQL)
	if err = viewlet.discoverTables(ctx, db, SQL); err != nil {
		return err
	}

	if viewlet.Table != nil && viewlet.Table.OutputJSONHint != "" {
		if err = viewlet.mergeTableJSONHint(viewlet.Table.OutputJSONHint); err != nil {
			return err
		}
	}

	aTemplate, err := parser.NewTemplate(viewlet.SQL, &viewlet.Resource.State)
	if err != nil {
		return fmt.Errorf("invalid DSQL: %w, %s", err, SQL)
	}
	aTemplate.DetectTypes(viewlet.UpdateParameterType)
	viewlet.SanitizedSQL = aTemplate.Sanitize()
	viewlet.View.Name = viewlet.Name
	return nil
}

func (s *Service) DefaultConnector() string {
	return s.Repository.Connectors[0].Name
}

// buildQueryViewletType build SQL/Table specification (field/column/keys) type
func (s *Service) buildQueryViewletType(ctx context.Context, viewlet *Viewlet) error {
	db, err := s.Repository.LookupDb(viewlet.Connector)
	if err != nil {
		return err
	}
	if viewlet.Table == nil {
		if err = s.initReaderViewlet(ctx, viewlet); err != nil {
			return err
		}
	}
	if viewlet.Expanded, err = viewlet.Resource.expandSQL(viewlet); err != nil {
		return err
	}
	return s.buildViewletType(ctx, db, viewlet)
}

func (s *Service) buildViewletType(ctx context.Context, db *sql.DB, viewlet *Viewlet) (err error) {
	if viewlet.Spec, err = inference.NewSpec(ctx, db, &s.Repository.Messages, viewlet.Table.Name, viewlet.Expanded.Query, viewlet.Expanded.Args...); err != nil {
		return fmt.Errorf("failed to create spec for %v, %w", viewlet.Name, err)
	}

	pkg := viewlet.Resource.rule.Package()
	viewlet.Spec.Package = pkg
	viewlet.Spec.Namespace = viewlet.Name
	cardinality := state.Many
	if err = viewlet.Spec.BuildType(pkg, viewlet.Name, cardinality, viewlet.whitelistMap(), nil); err != nil {
		return err
	}
	return nil
}

func (s *Service) Init(ctx context.Context) error {
	return s.Repository.Init(ctx)
}

func (s *Service) buildRouterResource(ctx context.Context, resource *Resource) (*router.Resource, error) {
	result := &router.Resource{}
	if resource.Rule.Cache != nil {
		s.Repository.Caches.Append(resource.Rule.Cache)
	}
	if len(s.Repository.Caches) > 0 {
		resource.Rule.With = append(resource.Rule.With, "cache")
	}
	if len(s.Repository.MessageBuses) > 0 {
		resource.Rule.With = append(resource.Rule.With, "mbus")
	}
	for k := range s.Repository.Substitutes {
		resource.Rule.With = append([]string{k}, resource.Rule.With...)
	}

	if resource.repository.ConstURL != "" {
		_, name := url.Split(resource.repository.ConstURL, file.Scheme)
		if ext := spath.Ext(name); ext != "" {
			name = name[:len(name)-len(ext)]
		}
		resource.Rule.With = append(resource.Rule.With, name)
	}

	result.With = resource.Rule.With
	if err := s.handleCustomTypes(ctx, resource); err != nil {
		return nil, err
	}
	s.adjustModulePackage(resource)
	result.Resource = &resource.Resource
	result.ColumnsDiscovery = true
	resource.Rule.applyRootViewRouteShorthands()
	route := &resource.Rule.Route
	if resource.Rule.Async != nil {
		rootView := resource.Rule.RootView()
		if rootView.Cache == nil { //also allow table dest in the future
			return nil, fmt.Errorf("cache setting is required with rasync")
		}
		resource.Rule.Async.WithCache = true
		setter.SetIntIfZero(&resource.Rule.Async.ExpiryTimeInSec, int((time.Millisecond * time.Duration(resource.Rule.Cache.TimeToLiveMs)).Seconds()))
		route.Async = resource.Rule.Async
	}
	routeMethods := strings.Split(route.Method, ",")

	if len(routeMethods) == 0 {
		routeMethods = []string{http.MethodGet}
	}

	switch len(routeMethods) {
	case 1:
		result.Routes = append(result.Routes, route)
	default:
		for _, method := range routeMethods {
			aRoute := *route
			aRoute.Method = method
			result.Routes = append(result.Routes, &aRoute)
		}
	}
	return result, nil
}

func (s *Service) adjustModulePackage(resource *Resource) {
	modLocation := resource.rule.GoModuleLocation()
	moduleURI := resource.rule.ModulePrefix
	var parent, prefix string
	goMod, err := s.fs.DownloadWithURL(context.Background(), url.Join(modLocation, "go.mod"))
	if err != nil {
		parent, prefix = url.Split(modLocation, file.Scheme)
		moduleURI = spath.Join(prefix, moduleURI)
		goMod, _ = s.fs.DownloadWithURL(context.Background(), url.Join(parent, "go.mod"))
	}

	if len(goMod) == 0 {
		return
	}

	modFile, _ := modfile.Parse("", goMod, nil)
	modulePath := spath.Join(modFile.Module.Mod.Path, moduleURI)
	for _, aType := range resource.Resource.Types {
		if aType.ModulePath != "" || aType.Package == "" {
			continue
		}
		if strings.HasSuffix(moduleURI, aType.Package) {
			aType.ModulePath = modulePath
		}
	}
}

func (s *Service) handleCustomTypes(ctx context.Context, resource *Resource) (err error) {
	resource.AdjustCustomType()
	if len(resource.CustomTypeURLs) == 0 {
		return nil
	}
	modLocation := resource.rule.GoModuleLocation()
	var info *plugin.Info
	URL := resource.CustomTypeURLs[0]
	customTypeLocation := url.Path(URL)
	if strings.Contains(customTypeLocation, modLocation) {
		modLocation = s.adjustModLocation(ctx, modLocation)
		info, err = plugin.NewInfo(ctx, modLocation)
	} else {
		info, err = plugin.NewInfo(ctx, URL)
	}
	if err != nil {
		return fmt.Errorf("failed to detect custom type: %v %w", URL, err)
	}

	if info.IntegrationMode == plugin.ModeStandalone {
		pluginCmd := &options.Plugin{}
		pluginCmd.Name = resource.rule.RuleName()
		pluginCmd.Source = append(pluginCmd.Source, URL)
		pluginCmd.Repository = s.Repository.Config.repository.RepositoryURL
		if err := pluginCmd.Init(); err != nil {
			return fmt.Errorf("failed to create standalone plugin for %v, %w", URL, err)
		}
		pluginCmd.BuildArgs = nil
		//pluginCmd.BuildArgs = []string{"'-gcflags \"all=-N -l\"'"}
		s.Plugins = append(s.Plugins, pluginCmd)
	}

	return nil
}

func (s *Service) updateComponentType(ctx context.Context, resource *Resource, parameters inference.State) error {
	if len(parameters) == 0 {
		return nil
	}
	for _, parameter := range parameters {
		aSignature, err := s.discoverComponentContract(ctx, resource, parameter.In)
		if err != nil {
			return fmt.Errorf("failed to discover component %v output type: %w", parameter.In.Name, err)
		}
		parameter.In.Name = aSignature.Method + ":" + aSignature.URI
		parameter.Schema = aSignature.Output.Clone()
		parameter.Schema.EnsurePointer()
		if aSignature.Input != nil {
			parameter.LocationInput = aSignature.Input

		}

		imps := aSignature.GoImports()
		for _, typeDef := range aSignature.Types {
			if err = extension.Config.Types.Register(typeDef.Name, xreflect.WithPackage(typeDef.Package), xreflect.WithTypeDefinition(typeDef.DataType), xreflect.WithGoImports(imps)); err != nil {
				return err
			}
		}
		for i := range aSignature.Types {
			resource.AppendTypeDefinition(aSignature.Types[i])
		}
	}

	for _, parameter := range parameters {
		resource.addParameterSchemaType(&parameter.Parameter)
	}
	return nil
}

func (s *Service) adjustModLocation(ctx context.Context, location string) string {
	if ok, _ := s.fs.Exists(ctx, spath.Join(location, "go.mod")); ok {
		return location
	}
	parent, _ := spath.Split(location)
	if ok, _ := s.fs.Exists(ctx, spath.Join(parent, "go.mod")); ok {
		return parent
	}
	return location
}

func New(config *Config, service afs.Service) *Service {
	ret := &Service{Repository: NewRepository(config), fs: service}
	return ret
}
