package cmd

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/toolbox/format"
	"log"
	"strings"
	"time"
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
	if options.SQLXLocation != "" && url.Scheme(options.SQLLocation, "e") == "e" {
		sourceURL := normalizeURL(options.SQLXLocation)
		SQL, err := fs.DownloadWithURL(context.Background(), sourceURL)
		if err != nil {
			return err
		}
		if xTable, err = ParseSQLx(string(SQL)); err != nil {
			log.Println(err)
		}
		if xTable != nil {
			updateGenerate(generate, xTable)
		}
	}
	aView := buildMainView(options, generate, route)
	if err := updateViewSQL(options, xTable, aView); err != nil {
		return err
	}

	var conn *view.Connector
	var ok bool
	connector := options.Connector

	if connector.DbName != "" {
		if conn, ok = connectors[connector.DbName]; !ok {
			if conn = connector.New(); conn.Name == connector.DbName {
				route.Resource.AddConnectors(conn)
			} else {
				return fmt.Errorf("undefined connector: %v", connector.DbName)
			}
		} else {
			route.Resource.AddConnectors(conn)
		}
		aView.Connector = &view.Connector{Reference: shared.Reference{Ref: connector.DbName}}
	}

	viewRoute := &router.Route{
		Method: "GET",
		Cors: &router.Cors{
			AllowCredentials: boolPtr(true),
			AllowHeaders:     stringsPtr("*"),
			AllowMethods:     stringsPtr("*"),
			AllowOrigins:     stringsPtr("*"),
			ExposeHeaders:    stringsPtr("*"),
		},
		URI:    config.APIPrefix + options.RouterURI(),
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
			if err := buildXRelations(options, route, viewRoute, xTable); err != nil {
				return err
			}
		}
		buildExcludeColumn(xTable, aView, viewRoute)

	}
	if len(options.Relations) > 0 && conn != nil {
		db, _ := conn.Db()
		meta := metadata.New()
		err := buildRelations(options, meta, db, route, aView, viewRoute)
		if err != nil {
			return err
		}
	}
	err := addParameters(options, route, aView)
	if err != nil {
		return err
	}

	route.Routes = append(route.Routes, viewRoute)

	route.With = []string{"connections"}
	dependency := &view.Resource{ModTime: time.Now()}
	dependency.Connectors = route.Resource.Connectors
	depURL := options.DepURL("connections")
	_ = fsAddYAML(fs, depURL, dependency)
	route.Resource.Connectors = nil
	return fsAddYAML(fs, options.RouterURL(), route)
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
		for _, except := range column.Except {
			holder := strings.Title(join.Table.Alias)
			viewRoute.Exclude = append(viewRoute.Exclude, holder+"."+viewCaser.Format(except, outputCaser))
		}
	}
}

func detectCaseFormat(xTable *Table) view.CaseFormat {
	var result = "lc"
	if len(xTable.Inner) > 0 {
		result = view.DetectCase(xTable.Inner[0].Name)
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
			},
		},
	}

	route.Resource.AddViews(aView)
	return aView
}

func updateGenerate(generate *Generate, table *Table) {
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
