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
	"os"
	"path"
)

func buildViewWithRouter(options *Options, config *standalone.Config, connectors map[string]*view.Connector) error {
	fs := afs.New()
	generate := options.Generate
	if generate.Name == "" {
		return nil
	}
	route := &router.Resource{
		Resource: &view.Resource{},
	}

	if options.SQLLocation != "" && url.Scheme(options.SQLLocation, "e") == "e" {
		parent, _ := url.Split(options.RouterURL(), file.Scheme)
		destURL := url.Join(parent, options.SQLLocation)
		baseDir, _ := os.Getwd()
		sourceURL := path.Join(baseDir, options.SQLLocation)
		if err := fs.Copy(context.Background(), sourceURL, destURL); err != nil {
			return err
		}

	}

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
		URI:   config.APIPrefix + options.RouterURI(),
		View:  &view.View{Reference: shared.Reference{Ref: aView.Name}},
		Index: router.Index{Namespace: map[string]string{}},
	}
	if options.Table != "" {
		viewRoute.Index.Namespace[options.Namespace()] = options.Generate.Name
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

	route.Resource.AddViews(aView)
	route.Routes = append(route.Routes, viewRoute)
	return fsAddYAML(fs, options.RouterURL(), route)
}
func stringsPtr(args ...string) *[]string {
	return &args
}

func boolPtr(b bool) *bool {
	return &b
}
