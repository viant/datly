package cmd

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/ast"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata"
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

	if s.options.SQLLocation != "" && url.Scheme(s.options.SQLLocation, "e") == "e" {
		parent, _ := url.Split(s.options.RouterURL(), file.Scheme)
		destURL := url.Join(parent, s.options.SQLLocation)
		sourceURL := normalizeURL(s.options.SQLLocation)
		if err := fs.Copy(context.Background(), sourceURL, destURL); err != nil {
			return err
		}
	}

	//_, _ = s.BuildRoute(ctx)
	//if err != nil {
	//	return err
	//}
	//
	//if err = route.Err(); err != nil {
	//	fmt.Println(err.Error())
	//}

	// ReadMode
	var xTable *option.Table

	// ReadMode
	var dataViewParams = make(map[string]*option.TableParam)
	routeOption := &option.Route{}
	// ExecMode
	var sqlExecModeView *option.ViewMeta
	var parameterHints []*option.ParameterHint
	if s.options.SQLXLocation != "" && url.Scheme(s.options.SQLLocation, "e") == "e" {
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
			if sqlExecModeView, err = ast.Parse(SQL, routeOption); err != nil {
				return err
			}
			s.updateMetaColumnTypes(ctx, sqlExecModeView, routeOption)
		} else {
			if xTable, dataViewParams, err = ParseSQLx(SQL, routeOption); err != nil {
				log.Println(err)
			}
			if xTable != nil {
				updateGenerateOption(generate, xTable)
			}
		}
	}
	s.buildDataParameters(dataViewParams, parameterHints, routeOption)

	aView := s.buildMainView(s.options, generate)
	if sqlExecModeView != nil {
		s.updateViewInSQLExecMode(aView, sqlExecModeView, routeOption)
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
		Output: router.Output{Style: router.Style(s.options.Output), Cardinality: view.Many, ResponseField: s.options.ResponseField()},
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
		buildExcludeColumn(xTable, aView, viewRoute)
	}

	s.buildDataViewParams(ctx, dataViewParams, routeOption, parameterHints)
	if len(s.options.Relations) > 0 {
		meta := metadata.New()
		err := s.buildRelations(ctx, meta, aView, viewRoute)
		if err != nil {
			return err
		}
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

func (s *serverBuilder) buildDataParameters(dataParameters map[string]*option.TableParam, parameters []*option.ParameterHint, routeOption *option.Route) error {
	if len(parameters) == 0 {
		return nil
	}

	for _, hintedParam := range parameters {
		if strings.HasPrefix(hintedParam.Parameter, "Unsafe.") {
			hintedParam.Parameter = strings.Replace(hintedParam.Parameter, "Unsafe.", "", 1)
		}
		paramName := hintedParam.Parameter
		aTable := &option.Table{}
		SQL, err := ast.UnmarshalHint(hintedParam.Hint, aTable)
		if err != nil {
			return err
		}

		if SQL == "" {
			continue
		}
		aTable.SQL = SQL
		if err := UpdateTableSettings(aTable, routeOption); err != nil {
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

func (s *serverBuilder) updateViewInSQLExecMode(aView *view.View, viewMeta *option.ViewMeta, route *option.Route) {
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
		if route.Method != http.MethodGet {
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
			table := &option.Table{Name: name, ColumnTypes: map[string]string{}}
			s.updateTableColumnTypes(ctx, table)
			for k, v := range table.ColumnTypes {
				viewMeta.ParameterTypes[k] = v
			}
		}
	}
	if len(viewMeta.Inserts) > 0 {

		for _, name := range viewMeta.Inserts {
			table := &option.Table{Name: name, ColumnTypes: map[string]string{}}
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

func (s *serverBuilder) buildDataViewParams(ctx context.Context, params map[string]*option.TableParam, routeOption *option.Route, hints []*option.ParameterHint) {
	if len(params) == 0 {
		return
	}

	for k, v := range params {
		table := v.Table

		if len(routeOption.Declare) > 0 {
			if len(table.ColumnTypes) == 0 {
				table.ColumnTypes = map[string]string{}
			}
			for k, v := range routeOption.Declare {
				table.ColumnTypes[k] = v
			}
		}

		if len(table.Inner) == 0 {
			fmt.Printf("Skpining data view params: %v - no column detected", v.Table)
			continue
		}

		var fields = make([]*view.Field, 0)
		s.updateTableColumnTypes(ctx, table)
		for _, column := range table.Inner {
			name := column.Alias
			if name == "" {
				name = column.Name
			}

			if name == "" {
				continue
			}

			dataType := column.DataType
			if dataType == "" {
				dataType = table.ColumnTypes[strings.ToLower(name)]
			}

			if dataType == "" {
				dataType = "string"
			}
			fields = append(fields, &view.Field{
				Name:   name,
				Embed:  false,
				Schema: &view.Schema{DataType: dataType},
			})
		}

		schemaName := strings.Title(k)
		s.route.Resource.Types = append(s.route.Resource.Types, &view.Definition{
			Name:   schemaName,
			Fields: fields,
		})

		relView := &view.View{
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

		if _, err := s.addViewConn(s.options.Connector.DbName, relView); err != nil {
			continue
		}

		s.mergeParamTypes(table)

		if err := s.updateView(ctx, table, relView); err != nil {
			continue
		}

		s.route.Resource.AddViews(relView)
		s.route.Resource.AddParameters(v.Param)
		if v.Table.Parameter != nil {
			mergeParameter(s.route, v.Table.Parameter)
		}
		mergeParameter(s.route, v.Param)
	}
}

func (s *serverBuilder) mergeParamTypes(table *option.Table) {
	if len(table.ColumnTypes) > 0 {
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

func buildExcludeColumn(xTable *option.Table, aView *view.View, viewRoute *router.Route) {
	joins := xTable.Joins.Index()
	viewCaser, _ := aView.CaseFormat.Caser()
	outputCaser, _ := format.NewCase(string(viewRoute.CaseFormat))

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
		Name:    generate.Name,
		Table:   generate.Table,
		FromURL: options.SQLLocation,
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
