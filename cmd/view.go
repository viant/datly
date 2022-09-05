package cmd

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/cmd/ast"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/router"
	"github.com/viant/datly/sanitizer"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox/format"
	"log"
	"net/http"
	"strings"
)

func (s *serverBuilder) buildViewWithRouter(ctx context.Context, config *standalone.Config) error {
	fs := afs.New()
	generate := &s.options.Generate
	if generate.Name == "" {
		return nil
	}

	s.route = &router.Resource{
		ColumnsDiscovery: true,
		Resource:         &view.Resource{},
	}

	// ReadMode
	var xTable *option.Table

	// ReadMode
	var dataViewParams = make(map[string]*option.TableParam)
	routeOption := &option.Route{}
	// ExecMode
	var sqlExecModeView *option.ViewMeta
	var parameterHints option.ParameterHints
	if s.options.SQLXLocation != "" {

		sourceURL := normalizeURL(s.options.SQLXLocation)
		SQLData, err := fs.DownloadWithURL(context.Background(), sourceURL)
		if err != nil {
			return err
		}

		SQL, uriParams, err := extractSetting(string(SQLData), routeOption)
		if err != nil {
			return fmt.Errorf("invalid settings: %w", err)
		}

		routeOption.URIParams = uriParams
		parameterHints = ast.ExtractParameterHints(SQL)

		if len(parameterHints) > 0 {
			SQL = ast.RemoveParameterHints(SQL, parameterHints)
		}

		if ast.IsSQLExecMode(SQL) {
			if sqlExecModeView, err = ast.Parse(SQL, routeOption, parameterHints); err != nil {
				return err
			}

			s.updateMetaColumnTypes(ctx, sqlExecModeView, routeOption)
			dataViewParams, err = extractDataViewParams(sqlExecModeView.Parameters, routeOption, parameterHints)
			if err != nil {
				return err
			}

		} else {
			if xTable, dataViewParams, err = ParseSQLx(SQL, routeOption, parameterHints); err != nil {
				log.Println(err)
			}

			if xTable != nil {
				updateGenerateOption(generate, xTable)
				s.Columns = xTable.Columns
				if xTable.Alias != "" {
					s.mainStarExpNamesapce = xTable.Alias
				}
			}
		}
	}

	if err := s.buildDataParametersFromHintedParamters(dataViewParams, parameterHints, routeOption); err != nil {
		log.Printf("failed to build data params: %v", err.Error())
	}

	aView := s.buildMainView(s.options, generate)
	if sqlExecModeView != nil {
		s.updateViewInSQLExecMode(aView, sqlExecModeView, dataViewParams, routeOption)
	}

	if _, err := s.addViewConn(s.options.Connector.DbName, aView); err != nil {
		return err
	}

	connectorRegistry := s.options.Connector.Registry()
	if len(connectorRegistry) > 0 {
		for k := range connectorRegistry {
			s.route.Resource.AddConnectors(connectorRegistry[k])
			s.connectors[k] = connectorRegistry[k]
		}
	}

	if err := s.updateView(ctx, xTable, aView); err != nil {
		return err
	}
	output := s.buildRouterOutput(xTable)
	viewRoute := &router.Route{
		Method:      "GET",
		EnableAudit: true,
		Cors: &router.Cors{
			AllowCredentials: boolPtr(true),
			AllowHeaders:     stringsPtr("*"),
			AllowMethods:     stringsPtr("*"),
			AllowOrigins:     stringsPtr("*"),
			ExposeHeaders:    stringsPtr("*"),
		},
		URI:    config.APIPrefix + s.options.RouterURI(routeOption.URI),
		View:   &view.View{Reference: shared.Reference{Ref: aView.Name}},
		Index:  router.Index{Namespace: map[string]string{}},
		Output: output,
	}

	if routeOption.Method != "" {
		viewRoute.Method = routeOption.Method
	}

	if sqlExecModeView != nil {
		if routeOption.Method == "" {
			viewRoute.Method = http.MethodPost
		}
		viewRoute.Service = router.ExecutorServiceType
	}
	if s.options.RedirectSizeKb > 0 && s.options.RedirectURL != "" {
		s.route.Redirect = &router.Redirect{TimeToLiveMs: 10000, MinSizeKb: s.options.RedirectSizeKb, StorageURL: s.options.RedirectURL}
	}

	if s.options.Table != "" {
		viewRoute.Index.Namespace[s.options.Namespace()] = s.options.Generate.Name
	}

	viewRoute.CaseFormat = "lc"
	if xTable != nil {
		aView.CaseFormat = detectCaseFormat(xTable)
		if len(xTable.Joins) > 0 {
			if err := s.buildXRelations(ctx, viewRoute, xTable); err != nil {
				return err
			}
		}
	}

	s.buildDataViewParams(ctx, dataViewParams, routeOption, parameterHints, viewRoute)
	if xTable != nil {
		caser, _ := aView.CaseFormat.Caser()
		s.buildExcludeColumn(xTable, caser, viewRoute)
	}

	updateURIParams(s.route, routeOption)
	updateParamReferences(s.route)
	s.route.Routes = append(s.route.Routes, viewRoute)

	s.route.With = []string{"connections"}
	if routeOption.Cache != nil {
		s.route.With = append(s.route.With, "cache")
		cacheDependency := &view.Resource{ModTime: TimeNow()}
		cacheURL := s.options.DepURL("cache")
		cacheDependency.CacheProviders = append(cacheDependency.CacheProviders, routeOption.Cache)
		_ = fsAddYAML(fs, cacheURL, cacheDependency)
	}

	dependency := &view.Resource{ModTime: TimeNow()}
	dependency.Connectors = s.route.Resource.Connectors
	depURL := s.options.DepURL("connections")
	_ = fsAddYAML(fs, depURL, dependency)
	s.route.Resource.Connectors = nil
	return fsAddYAML(fs, s.options.RouterURL(), s.route)
}

func (s *serverBuilder) buildRouterOutput(xTable *option.Table) router.Output {
	output := router.Output{}
	if len(s.Columns) == 0 {
		return output
	}

	startExpr := s.Columns.StarExpr(xTable.Alias)
	if startExpr != nil {
		if comments := startExpr.Comments; comments != "" {
			if _, err := ast.UnmarshalHint(comments, &output); err != nil {
				fmt.Printf("err: %v\n", err)
			}
		}
	}

	if output.Style == "" {
		output.Style = router.Style(s.options.Output)
	}
	if output.Cardinality == "" {
		output.Cardinality = view.Many
	}
	if output.ResponseField == "" {
		output.ResponseField = s.options.ResponseField()
	}
	return output
}

func (s *serverBuilder) buildDataParametersFromHintedParamters(dataParameters map[string]*option.TableParam, parameters option.ParameterHints, routeOption *option.Route) error {
	if len(parameters) == 0 {
		return nil
	}

	for _, hintedParam := range parameters {

		_, paramName := sanitizer.GetHolderName(hintedParam.Parameter)
		aTable := option.NewTable("")
		SQL, err := ast.UnmarshalHint(hintedParam.Hint, aTable)
		if err != nil {
			return err
		}

		if SQL == "" {
			continue
		}
		aTable.SQL = SQL
		if err := UpdateTableSettings(aTable, routeOption, parameters); err != nil {
			return err
		}

		aTable.Alias = paramName
		if aTable.DataViewParameter == nil {
			aTable.DataViewParameter = &view.Parameter{}
		}

		aTable.DataViewParameter.In = &view.Location{Name: paramName, Kind: view.DataViewKind}
		aTable.DataViewParameter.Schema = &view.Schema{Name: strings.Title(paramName)}
		aTable.DataViewParameter.Name = paramName
		dataParameters[paramName] = &option.TableParam{Table: aTable, Param: aTable.DataViewParameter}
		UpdateAuthToken(aTable)
	}
	return nil
}

func (s *serverBuilder) updateViewInSQLExecMode(aView *view.View, viewMeta *option.ViewMeta, params map[string]*option.TableParam, route *option.Route) {
	aView.Mode = view.Mode(viewMeta.Mode)
	aView.Template = &view.Template{
		Source:     viewMeta.Source,
		Parameters: []*view.Parameter{},
	}

	if len(viewMeta.Updates) > 0 {
		aView.Table = viewMeta.Updates[0]
	} else if len(viewMeta.Inserts) > 0 {
		aView.Table = viewMeta.Inserts[0]
	}

	for _, p := range viewMeta.Parameters {
		var dataType string
		if p.Typer != nil {
			switch actual := p.Typer.(type) {
			case *option.ColumnType:
				dataType = viewMeta.ParameterTypes[strings.ToLower(actual.ColumnName)]
			case *option.LiteralType:
				dataType = actual.RType.String()
			}
		}

		if dataType == "" {
			dataType = viewMeta.ParameterTypes[strings.ToLower(p.Name)]
		}

		if dataType == "" {
			dataType = "string"
		}

		if p.Repeated {
			dataType = "[]" + dataType
		}

		p.DataType = dataType

		metaParameter := convertMetaParameter(p)
		if _, ok := params[p.Name]; ok {
			metaParameter.In.Kind = view.DataViewKind
		} else if route.Method != http.MethodGet {
			metaParameter.In.Kind = view.RequestBodyKind
		} else {
			metaParameter.In.Kind = view.QueryKind
		}

		aView.Template.Parameters = append(aView.Template.Parameters, metaParameter)
	}
}

func (s *serverBuilder) updateMetaColumnTypes(ctx context.Context, viewMeta *option.ViewMeta, routeOption *option.Route) {

	if len(viewMeta.ParameterTypes) == 0 {
		viewMeta.ParameterTypes = map[string]string{}
	}
	if len(viewMeta.Updates) > 0 {

		for _, name := range viewMeta.Updates {
			table := option.NewTable(name)
			s.updateTableColumnTypes(ctx, table)
			for k, v := range table.ColumnTypes {
				viewMeta.ParameterTypes[k] = v
			}
		}
	}
	if len(viewMeta.Inserts) > 0 {

		for _, name := range viewMeta.Inserts {
			table := option.NewTable(name)
			s.updateTableColumnTypes(ctx, table)
			for k, v := range table.ColumnTypes {
				viewMeta.ParameterTypes[k] = v
			}
		}
	}
	if len(routeOption.Declare) > 0 {
		for k, v := range viewMeta.ParameterTypes {
			viewMeta.ParameterTypes[k] = v
		}
	}
}

func extractSetting(SQL string, route *option.Route) (string, map[string]bool, error) {
	hint := ast.ExtractHint(SQL)
	if hint == "" {
		return SQL, map[string]bool{}, nil
	}

	SQL = strings.Replace(SQL, hint, "", 1)

	_, err := ast.UnmarshalHint(hint, route)
	if err != nil {
		return SQL, map[string]bool{}, err
	}

	uriParams := extractURIParams(route.URI)

	return SQL, uriParams, nil
}

func extractURIParams(URI string) map[string]bool {
	result := map[string]bool{}

	if URI == "" {
		return result
	}

	uriParams := ast.ParseURIParams(URI)
	for _, param := range uriParams {
		result[param] = true
	}

	return result
}

func (s *serverBuilder) buildDataViewParams(ctx context.Context, params map[string]*option.TableParam, routeOption *option.Route, hints option.ParameterHints, route *router.Route) {
	if len(params) == 0 {
		return
	}

	for k, v := range params {

		if isMetaTemplate(v.Table.Name) {
			s.buildViewMetaTemplate(k, v)
			continue
		}
		schemaName := strings.Title(k)
		typeDef, _ := s.BuildSchema(ctx, schemaName, k, v, routeOption)
		if typeDef != nil {
			s.route.Resource.Types = append(s.route.Resource.Types, typeDef)
		}

		relView, err := s.buildParamView(ctx, routeOption, k, schemaName, v, hints)
		if err != nil {
			fmt.Printf("unable to create data view param: %v\n", err.Error())
			continue
		}

		if _, err := s.addViewConn(s.options.Connector.DbName, relView); err != nil {
			continue
		}

		s.mergeParamTypes(v.Table)

		s.route.Resource.AddViews(relView)
		s.route.Resource.AddParameters(v.Param)
		if v.Table.Parameter != nil {
			mergeParameter(s.route, v.Table.Parameter)
		}
		mergeParameter(s.route, v.Param)
	}
}

func isMetaTemplate(candidate string) bool {
	return strings.Contains(candidate, "$View.") && strings.Contains(candidate, ".SQL")
}

func getMetaTemplateHolder(name string) string {
	var viewNs = "$View."
	index := strings.Index(name, viewNs)
	name = name[index+len(viewNs):]
	index = strings.Index(name, ".SQL")
	return name[:index]
}

func (s *serverBuilder) buildParamViewWithoutTemplate(k string, v *option.TableParam, schemaName string) *view.View {
	return &view.View{
		Name:   k,
		Table:  v.Table.Name,
		Schema: &view.Schema{Name: schemaName},
		Selector: &view.Config{
			Limit: 25,
			Constraints: &view.Constraints{
				Limit:  true,
				Offset: true,
			},
		},
	}
}

func (s *serverBuilder) mergeParamTypes(table *option.Table) {
	if len(table.ColumnTypes) > 0 {
		if table.ViewMeta.ParameterTypes == nil {
			table.ViewMeta.ParameterTypes = map[string]string{}
		}

		for k, v := range table.ColumnTypes {
			if len(table.ViewMeta.ParameterTypes) == 0 {
				table.ViewMeta.ParameterTypes = map[string]string{}
			}
			table.ViewMeta.ParameterTypes[k] = v
		}
	}
}

func updateParamReferences(route *router.Resource) {
	var resourceParams = map[string]*view.Parameter{}
	if len(route.Resource.Parameters) > 0 {
		for i, param := range route.Resource.Parameters {
			resourceParams[param.Name] = route.Resource.Parameters[i]
		}
	}
	for _, aView := range route.Resource.Views {
		if aView.Template == nil || len(aView.Template.Parameters) == 0 {
			continue
		}
		for i, viewParam := range aView.Template.Parameters {
			if resourceParam, ok := resourceParams[viewParam.Name]; ok {
				updateParamPrecedence(resourceParam, viewParam)
			} else {
				route.Resource.Parameters = append(route.Resource.Parameters, aView.Template.Parameters[i])
			}
			aView.Template.Parameters[i] = &view.Parameter{Reference: shared.Reference{Ref: viewParam.Name}}
		}
	}

}

func updateParamPrecedence(dest *view.Parameter, source *view.Parameter) {
	dest.Required = boolPtr(dest.IsRequired() || source.IsRequired())

	if source.DateFormat != "" && dest.DateFormat == "" {
		dest.DateFormat = source.DateFormat
	}

	if dest.Schema.Cardinality != view.Many {
		dest.Schema.Cardinality = source.Schema.Cardinality
	}

	if dest.Schema.Name == "" {
		dest.Schema.Name = source.Schema.Name
	}
	dest.Schema.DataType = source.Schema.DataType

	if dest.Codec == nil {
		dest.Codec = source.Codec
	}

	if source.In != nil {
		dest.In = source.In
	}

	if dest.ErrorStatusCode == 0 && source.ErrorStatusCode != 0 {
		dest.ErrorStatusCode = source.ErrorStatusCode
	}

	if dest.Codec == nil {
		dest.Codec = source.Codec
	}
}

func updateURIParams(route *router.Resource, setting *option.Route) {
	if setting == nil || len(setting.URIParams) == 0 {
		return
	}

	for _, aView := range route.Resource.Views {
		if aView.Template == nil || len(aView.Template.Parameters) == 0 {
			continue
		}
		for _, viewParam := range aView.Template.Parameters {
			if _, ok := setting.URIParams[viewParam.Name]; ok {
				viewParam.In.Kind = view.PathKind
			}
		}
	}
}

func mergeParameter(route *router.Resource, param *view.Parameter) {
	for _, aView := range route.Resource.Views {
		if aView.Template == nil || len(aView.Template.Parameters) == 0 {
			continue
		}
		for i, viewParam := range aView.Template.Parameters {
			if viewParam.Name != param.Name {
				continue
			}

			updateParamPrecedence(aView.Template.Parameters[i], param)
		}
	}
}

func (s *serverBuilder) addViewConn(connectorName string, aView *view.View) (*view.Connector, error) {
	if connectorName == "" {
		return nil, nil
	}

	connector := s.options.Connector
	conn := connector.New()
	viewConnector := &view.Connector{}
	viewConnector.Ref = connector.DbName

	_, ok := s.connectors[connector.DbName]

	if !ok {
		s.route.Resource.AddConnectors(conn)
	}

	aView.Connector = viewConnector
	return conn, nil
}

func (s *serverBuilder) buildExcludeColumn(xTable *option.Table, viewCaser format.Case, viewRoute *router.Route) {
	joins := xTable.Joins.Index()
	outputCaser, _ := format.NewCase(string(viewRoute.CaseFormat))
	//columnsIndex := xTable.Columns.Index()

	for _, column := range xTable.Columns {
		if len(column.Except) == 0 {
			continue
		}
		if column.Ns == xTable.Alias {
			for _, except := range column.Except {
				viewRoute.Exclude = append(viewRoute.Exclude, viewCaser.Format(except, outputCaser))
			}
			continue
		}

		join := joins[column.Ns]
		if join != nil && join.Table != nil {
			for _, except := range column.Except {
				holder := strings.Title(join.Table.Alias)
				viewRoute.Exclude = append(viewRoute.Exclude, holder+"."+viewCaser.Format(except, outputCaser))
			}
		}

		for _, aView := range s.route.Resource.Views {
			templateMeta := aView.Template.Meta
			if templateMeta == nil || column.Ns != templateMeta.Name {
				continue
			}

			if _, ok := joins[aView.Name]; !ok {
				continue
			}

			for _, except := range column.Except {
				actual, _ := format.NewCase(view.DetectCase(except))
				viewRoute.Exclude = append(viewRoute.Exclude, column.Ns+"."+actual.Format(except, outputCaser))
			}
		}
	}
}

func detectCaseFormat(xTable *option.Table) view.CaseFormat {
	names := make([]string, 0)
	for _, column := range xTable.Inner {
		columnName := column.Alias
		if columnName == "" {
			columnName = column.Name
		}

		if columnName == "*" {
			continue
		}

		names = append(names, columnName)
	}

	if len(names) == 0 && len(xTable.ColumnTypes) > 0 {
		for columnName := range xTable.ColumnTypes {
			names = append(names, columnName)
		}
	}

	if len(names) == 0 {
		names = append(names, xTable.Name)
	}

	return view.CaseFormat(view.DetectCase(names...))
}

func (s *serverBuilder) buildMainView(options *Options, generate *Generate) *view.View {
	aView := &view.View{
		Name:  generate.Name,
		Table: generate.Table,
		Selector: &view.Config{
			Limit: 25,
			Constraints: &view.Constraints{
				Filterable: []string{"*"},
				Criteria:   true,
				Limit:      true,
				Offset:     true,
				Projection: true,
			},
		},
		Connector: &view.Connector{Reference: shared.Reference{Ref: options.DbName}},
	}

	s.route.Resource.AddViews(aView)
	return aView
}

func updateGenerateOption(generate *Generate, table *option.Table) {
	if table == nil {
		return
	}
	table.Ref = generate.Name
	generate.Table = table.Name
}

func stringsPtr(args ...string) *[]string {
	return &args
}

func boolPtr(b bool) *bool {
	return &b
}
