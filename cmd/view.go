package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/ast"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/format"
	"log"
	"strings"
)

func buildViewWithRouter(options *Options, config *standalone.Config, connectors map[string]*view.Connector) error {
	fs := afs.New()
	generate := &options.Generate
	if generate.Name == "" {
		return nil
	}
	route := &router.Resource{
		Resource: &view.Resource{},
	}
	if options.SQLLocation != "" && url.Scheme(options.SQLLocation, "e") == "e" {
		parent, _ := url.Split(options.RouterURL(), file.Scheme)
		destURL := url.Join(parent, options.SQLLocation)
		sourceURL := normalizeURL(options.SQLLocation)
		if err := fs.Copy(context.Background(), sourceURL, destURL); err != nil {
			return err
		}
	}

	var xTable *Table
	var dataViewParams map[string]*TableParam
	routeSetting := &RouteSetting{}
	if options.SQLXLocation != "" && url.Scheme(options.SQLLocation, "e") == "e" {
		sourceURL := normalizeURL(options.SQLXLocation)
		SQLData, err := fs.DownloadWithURL(context.Background(), sourceURL)
		if err != nil {
			return err
		}

		SQL := strings.TrimSpace(string(SQLData))
		SQL = extractSetting(strings.TrimSpace(string(SQLData)), routeSetting)

		toolbox.Dump(routeSetting)

		if xTable, dataViewParams, err = ParseSQLx(SQL); err != nil {
			log.Println(err)
		}
		if xTable != nil {
			updateGenerateOption(generate, xTable)
		}
	}

	aView := buildMainView(options, generate, route)
	_, err := addViewConn(options, connectors, route, aView)
	if err != nil {
		return err
	}
	connectorRegistry := options.Connector.Registry()
	if len(connectorRegistry) > 0 {
		for k := range connectorRegistry {
			route.Resource.AddConnectors(connectorRegistry[k])
			connectors[k] = connectorRegistry[k]
		}
	}

	if err := updateView(options, xTable, aView); err != nil {
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
		URI:    config.APIPrefix + options.RouterURI(routeSetting.URI),
		View:   &view.View{Reference: shared.Reference{Ref: aView.Name}},
		Index:  router.Index{Namespace: map[string]string{}},
		Output: router.Output{Style: router.Style(options.Output), Cardinality: view.Many, ResponseField: options.ResponseField()},
	}
	if options.RedirectSizeKb > 0 && options.RedirectURL != "" {
		route.Redirect = &router.Redirect{TimeToLiveMs: 10000, MinSizeKb: options.RedirectSizeKb, StorageURL: options.RedirectURL}
	}

	if options.Table != "" {
		viewRoute.Index.Namespace[options.Namespace()] = options.Generate.Name
	}
	viewRoute.CaseFormat = "lc"
	if xTable != nil {
		aView.CaseFormat = detectCaseFormat(xTable)
		if len(xTable.Joins) > 0 {
			if err := buildXRelations(options, connectors, route, viewRoute, xTable); err != nil {
				return err
			}
		}
		buildExcludeColumn(xTable, aView, viewRoute)
	}

	buildDataViewParams(options, connectors, dataViewParams, route)
	if len(options.Relations) > 0 {
		meta := metadata.New()
		err := buildRelations(options, meta, connectors, route, aView, viewRoute)
		if err != nil {
			return err
		}
	}

	updateURIParams(route, routeSetting)

	route.Routes = append(route.Routes, viewRoute)

	route.With = []string{"connections"}
	dependency := &view.Resource{ModTime: TimeNow()}
	dependency.Connectors = route.Resource.Connectors
	depURL := options.DepURL("connections")
	_ = fsAddYAML(fs, depURL, dependency)
	route.Resource.Connectors = nil
	return fsAddYAML(fs, options.RouterURL(), route)
}

func extractSetting(SQL string, settings *RouteSetting) string {
	if strings.HasPrefix(SQL, "/*") {
		index := strings.Index(SQL, "*/")
		routeSetting := SQL[3:index]
		SQL = SQL[index+3:]
		err := json.Unmarshal([]byte(routeSetting), settings)
		if err != nil {
			log.Printf("invalid route setting: %s, %w", routeSetting, err)
		}
		if settings.URI != "" {
			if params := ast.ParseURIParams(settings.URI); len(params) > 0 {
				settings.URIParams = map[string]bool{}
				for _, param := range params {
					settings.URIParams[param] = true
				}

			}
		}

	}
	return SQL
}

func buildDataViewParams(options *Options, connectors map[string]*view.Connector, params map[string]*TableParam, route *router.Resource) {
	if len(params) == 0 {
		return
	}

	for k, v := range params {
		table := v.Table
		if len(table.Inner) == 0 {
			fmt.Printf("Skpining data view params: %v - no column detected", v.Table)
			continue
		}
		var fields = make([]*view.Field, 0)
		updateTableColumnTypes(options, table)
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
				dataType = "string"
			}
			fields = append(fields, &view.Field{
				Name:   name,
				Embed:  false,
				Schema: &view.Schema{DataType: dataType},
			})
		}

		route.Resource.Types = append(route.Resource.Types, &view.Definition{
			Name:   strings.Title(k),
			Fields: fields,
		})
		relView := &view.View{
			Name:  k,
			Table: v.Table.Name,
			Selector: &view.Config{
				Limit: 1,
			},
		}
		if _, err := addViewConn(options, connectors, route, relView); err != nil {
			continue
		}
		if err := updateView(options, table, relView); err != nil {
			continue
		}

		route.Resource.AddViews(relView)
		route.Resource.AddParameters(v.Param)
		if v.Table.Parameter != nil {
			mergeParameter(route, v.Table.Parameter)
		}
		mergeParameter(route, v.Param)
	}
}

func updateURIParams(route *router.Resource, setting *RouteSetting) {
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
			aView.Template.Parameters[i].In = param.In
			aView.Template.Parameters[i].Schema = param.Schema
			aView.Template.Parameters[i].Codec = param.Codec
			aView.Template.Parameters[i].ErrorStatusCode = param.ErrorStatusCode
		}
	}
}

func addViewConn(options *Options, connectors map[string]*view.Connector, route *router.Resource, aView *view.View) (*view.Connector, error) {
	connector := options.Connector
	if connector.DbName == "" {
		return nil, nil
	}
	conn := connector.New()
	viewConnector := &view.Connector{}
	viewConnector.Ref = connector.DbName

	_, ok := connectors[connector.DbName]
	if !ok {
		route.Resource.AddConnectors(conn)
	}
	aView.Connector = viewConnector
	return conn, nil
}

func buildExcludeColumn(xTable *Table, aView *view.View, viewRoute *router.Route) {
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

func detectCaseFormat(xTable *Table) view.CaseFormat {
	var result = "lc"
	if len(xTable.Inner) > 0 {
		for _, candidate := range xTable.Inner {
			if len(candidate.Name) > 3 {
				result = view.DetectCase(xTable.Inner[0].Name)
				break
			}
		}
	}
	return view.CaseFormat(result)

}

func buildMainView(options *Options, generate *Generate, route *router.Resource) *view.View {
	aView := &view.View{
		Name:    generate.Name,
		Table:   generate.Table,
		FromURL: options.SQLLocation,
		Selector: &view.Config{
			Limit: 40,
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
	route.Resource.AddViews(aView)
	return aView
}

func updateGenerateOption(generate *Generate, table *Table) {
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
