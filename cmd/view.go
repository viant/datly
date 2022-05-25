package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/gateway/registry"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"reflect"
	"strings"
)

func buildViewWithRouter(options *Options, config *standalone.Config, connectors map[string]*view.Connector) error {
	generate := options.Generate
	if generate.Name == "" {
		return nil
	}
	route := &router.Resource{
		Resource: &view.Resource{},
	}
	aView := &view.View{
		Name:  generate.Name,
		Table: generate.Table,
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
	return fsAddYAML(afs.New(), options.RouterURL(), route)
}

func addParameters(options *Options, route *router.Resource, aView *view.View) error {
	if len(options.Parameters) > 0 {
		var parameters = []*view.Parameter{}
		for _, param := range options.Parameters {
			if !strings.Contains(param, ":") {
				return fmt.Errorf("invalid param: %v, expected format: name:type", param)
			}
			pair := strings.SplitN(param, ":", 2)
			aParam := &view.Parameter{
				Name: pair[0],
				In: &view.Location{
					Kind: view.QueryKind,
				},
				Schema: &view.Schema{
					DataType: pair[1],
				},
			}

			switch pair[1] {
			case "int":
				registry.Types[pair[1]] = reflect.TypeOf(0)
			case "string":
				registry.Types[pair[1]] = reflect.TypeOf("")

			}
			parameters = append(parameters, aParam)
		}
		from := aView.From
		if from == "" {
			from = "SELECT * FROM " + aView.Table
		}
		aView.Template = &view.Template{Parameters: parameters, Source: from}
	}
	return nil
}

func buildRelations(options *Options, meta *metadata.Service, db *sql.DB, route *router.Resource, aView *view.View, viewRoute *router.Route) error {
	pk := []sink.Key{}
	if err := meta.Info(context.Background(), db, info.KindPrimaryKeys, &pk, option.NewArgs("", options.Connector.DbName, options.Table)); err == nil && len(pk) > 0 {
		for _, rel := range options.Relations {
			if !strings.Contains(rel, ":") {
				return fmt.Errorf("invalid relation: %v, expected name:table", rel)
			}
			pair := strings.SplitN(rel, ":", 2)
			relName := pair[0]
			relTable := pair[1]
			fk, err := readForeignKeys(options, meta, db, relTable)
			if err != nil {
				fmt.Printf("skiping relation: %v due to %w", rel, err)
				continue
			}
			relView := &view.View{
				Name:  relName,
				Table: relTable,
				Selector: &view.Config{
					Limit: 40,
				},
			}
			route.Resource.AddViews(relView)
			aView.With = append(aView.With, &view.Relation{
				Name: aView.Name + relName,
				Of: &view.ReferenceView{
					View:   view.View{Reference: shared.Reference{Ref: relName}, Name: relName + "#"},
					Column: fk[0].Column,
				},
				Cardinality: view.Many,
				Column:      fk[0].ReferenceColumn,
				Holder:      strings.Title(relName),
			})

			viewRoute.Index.Namespace[namespace(relTable)] = relName + "#"
		}
	}
	return nil
}

func readForeignKeys(options *Options, meta *metadata.Service, db *sql.DB, relTable string) ([]sink.Key, error) {
	fk := []sink.Key{}
	err := meta.Info(context.Background(), db, info.KindForeignKeys, &fk, option.NewArgs("", options.Connector.DbName, relTable))
	if err != nil {
		return nil, err
	}
	var result = make([]sink.Key, 0)
	for i, candidate := range fk {
		if candidate.ReferenceTable == options.Table {
			result = append(result, fk[i])
		}
	}
	return result, err
}

func stringsPtr(args ...string) *[]string {
	return &args
}

func boolPtr(b bool) *bool {
	return &b
}
