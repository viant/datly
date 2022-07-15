package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/ast"
	"github.com/viant/datly/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/ast/parser"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	rdata "github.com/viant/toolbox/data"
	"github.com/viant/toolbox/format"
	"reflect"
	"strings"
)

func lookupView(resource *view.Resource, name string) *view.View {
	for _, candidate := range resource.Views {
		if candidate.Name == name {
			return candidate
		}
	}
	return nil
}

func buildXRelations(options *Options, route *router.Resource, viewRoute *router.Route, xTable *Table) error {
	if len(xTable.Joins) == 0 {
		return nil
	}
	for _, join := range xTable.Joins {
		relView := &view.View{
			Name:  join.Table.Alias,
			Table: join.Table.Name,
			Selector: &view.Config{
				Limit: 40,
			},
		}
		if err := updateView(options, join.Table, relView); err != nil {
			return err
		}

		route.Resource.AddViews(relView)
		var cardinality = view.Many
		if join.ToOne {
			cardinality = view.One
		}
		aView := lookupView(route.Resource, join.Owner.Ref)
		if aView == nil {
			return fmt.Errorf("failed to lookup view: %v", join.Owner.Name)
		}

		newCase, err := format.NewCase(view.DetectCase(join.Table.Alias))
		if err != nil {
			return err
		}

		withView := &view.Relation{
			Name: aView.Name + "_" + join.Table.Alias,
			Of: &view.ReferenceView{
				View:   view.View{Reference: shared.Reference{Ref: join.Table.Alias}, Name: join.Table.Alias + "#"},
				Column: join.Key,
				Field:  join.Field,
			},
			Cardinality: cardinality,
			Column:      join.OwnerKey,
			ColumnAlias: join.KeyAlias,
			Holder:      newCase.Format(join.Table.Alias, format.CaseUpperCamel),

			IncludeColumn: true,
		}
		if join.Connector != "" {
			relView.Connector = &view.Connector{Reference: shared.Reference{Ref: join.Connector}}
		}
		aView.With = append(aView.With, withView)

		viewRoute.Index.Namespace[namespace(join.Table.Alias)] = join.Table.Alias + "#"
	}
	return nil
}

func updateView(options *Options, table *Table, aView *view.View) error {
	if table == nil {
		return nil
	}
	fmt.Printf("Discovering  %v metadata ...\n", aView.Name)
	updateTableColumnTypes(options, table)
	updateParameterTypes(table)

	if err := updateColumnsConfig(table, aView); err != nil {
		return err
	}

	if viewMeta := table.ViewMeta; viewMeta != nil {
		var SQL string
		var err error
		if viewMeta.From == "" {
			SQL, err = createAndEvalauteTemplate(viewMeta)
		} else {
			SQL = viewMeta.From
		}

		if err != nil {
			return err
		}

		aView.UseBindingPositions = boolPtr(!viewMeta.HasVeltySyntax)

		fromURL := options.SQLURL(table.Alias + "_from")
		fs := afs.New()
		if err := fs.Upload(context.Background(), fromURL, file.DefaultFileOsMode, strings.NewReader(SQL)); err != nil {
			return err
		}
		_, URI := url.Split(fromURL, file.Scheme)
		aView.FromURL = URI

		if len(viewMeta.Parameters) > 0 || viewMeta.Source != "" {
			templateParameters := make([]*view.Parameter, len(viewMeta.Parameters))
			for i, parameter := range viewMeta.Parameters {
				positions := parameter.Positions
				if aView.UseBindingPositions != nil && !*aView.UseBindingPositions {
					positions = nil
				}
				if table != nil {
					if paramType, ok := table.ViewMeta.ParameterTypes[parameter.Name]; ok {
						parameter.Type = paramType
					}
				}
				templateParameters[i] = &view.Parameter{
					Name: parameter.Id,
					In: &view.Location{
						Kind: view.Kind(parameter.Kind),
						Name: parameter.Name,
					},
					Positions: positions,
					Required:  boolPtr(parameter.Required),
					Schema:    &view.Schema{DataType: parameter.Type},
				}
			}

			aView.Template = &view.Template{
				Source:     "",
				Parameters: templateParameters,
			}
		}

		if viewMeta.Source != "" {
			source := viewMeta.Source
			sourceURL := options.SQLURL(table.Alias + "_source")
			if err := fs.Upload(context.Background(), sourceURL, file.DefaultFileOsMode, strings.NewReader(source)); err != nil {
				return err
			}

			_, URI = url.Split(sourceURL, file.Scheme)
			aView.Template.SourceURL = URI
		}
	}

	return nil
}

func updateColumnsConfig(table *Table, aView *view.View) error {
	query, err := parser.ParseQuery(table.SQL)
	if err != nil {
		return nil
	}

	aView.ColumnsConfig = map[string]*view.ColumnConfig{}
	for _, item := range query.List {

		if item.Comments == "" {
			continue
		}

		configJSON := strings.TrimPrefix(item.Comments, "/*")
		configJSON = strings.TrimSuffix(configJSON, "*/")
		configJSON = strings.TrimSpace(configJSON)

		aConfig := &view.ColumnConfig{}
		if err := json.Unmarshal([]byte(configJSON), aConfig); err != nil {
			fmt.Printf(err.Error())
			continue
		}

		aView.ColumnsConfig[item.Alias] = aConfig
	}
	return nil
}

func createAndEvalauteTemplate(meta *ast.ViewMeta) (string, error) {
	schemaFields := make([]reflect.StructField, len(meta.Parameters))
	presenceFields := make([]reflect.StructField, len(meta.Parameters))

	expandMap := rdata.Map{}
	for i, parameter := range meta.Parameters {
		if paramType, ok := meta.ParameterTypes[parameter.Name]; ok {
			parameter.Type = paramType
		}
		var pkgPath string
		if parameter.Name[0] < 'A' || parameter.Name[0] > 'Z' {
			pkgPath = "github.com/viant/datly/cmd"
		}

		presenceFields[i] = reflect.StructField{
			Name:    parameter.Id,
			PkgPath: pkgPath,
			Type:    reflect.TypeOf(true),
		}

		paramType, err := view.ParseType(parameter.Type)
		if err != nil {
			return "", err
		}

		schemaFields[i] = reflect.StructField{
			Name:    parameter.Id,
			PkgPath: pkgPath,
			Type:    paramType,
		}

		var value interface{}
		switch paramType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			value = 0
		case reflect.Float32, reflect.Float64:
			value = 0.0
		case reflect.String:
			value = "''"
		case reflect.Bool:
			value = false
		default:
			value = reflect.New(paramType).Elem().Interface()
		}

		expandMap.SetValue(parameter.Id, value)
	}

	schemaType := reflect.StructOf(schemaFields)
	presenceType := reflect.StructOf(presenceFields)
	evaluator, err := view.NewEvaluator(schemaType, presenceType, meta.Source)
	if err != nil {
		return "", err
	}

	SQL, err := evaluator.Evaluate(schemaType, reflect.New(schemaType).Elem().Interface(), reflect.New(presenceType).Elem().Interface(), &view.Param{})
	if err != nil {
		return "", err
	}

	return expandMap.ExpandAsText(SQL), nil
}

func buildRelations(options *Options, meta *metadata.Service, connectors map[string]*view.Connector, route *router.Resource, aView *view.View, viewRoute *router.Route) error {
	pk := []sink.Key{}
	conn, ok := connectors[options.DbName]
	if !ok {
		return nil
	}
	db, err := conn.Db()
	if err != nil {
		return err
	}
	defer db.Close()
	if err := meta.Info(context.Background(), db, info.KindPrimaryKeys, &pk, option.NewArgs("", options.Connector.DbName, options.Table)); err == nil && len(pk) > 0 {
		for _, rel := range options.Relations {
			if !strings.Contains(rel, ":") {
				return fmt.Errorf("invalid relation: %v, expected name:table", rel)
			}
			pair := strings.SplitN(rel, ":", 3)
			relName := pair[0]
			relTable := pair[1]
			relationCardinality := view.Many
			if len(pair) > 2 {
				relationCardinality = view.Cardinality(pair[2])
			}

			if relationCardinality == "" {
				relationCardinality = view.Many
			}

			fk, err := readForeignKeys(options, meta, db, relTable, relationCardinality)
			if err != nil {
				fmt.Printf("skiping relation: %v due to %v", rel, err)
				continue
			}

			var childColumn, parentColumn string
			if relationCardinality == view.Many {
				parentColumn = fk[0].ReferenceColumn
				childColumn = fk[0].Column
			} else {
				childColumn = fk[0].ReferenceColumn
				parentColumn = fk[0].Column
			}

			relView := &view.View{
				Name:  relName,
				Table: relTable,
				Selector: &view.Config{
					Limit: 40,
				},
			}
			route.Resource.AddViews(relView)

			caseFormat, err := format.NewCase(view.DetectCase(relName))
			if err != nil {
				return err
			}

			aView.With = append(aView.With, &view.Relation{
				Name: aView.Name + relName,
				Of: &view.ReferenceView{
					View:   view.View{Reference: shared.Reference{Ref: relName}, Name: relName + "#"},
					Column: childColumn,
				},
				Cardinality: view.Many,
				Column:      parentColumn,
				Holder:      caseFormat.Format(relName, format.CaseUpperCamel),
			})

			viewRoute.Index.Namespace[namespace(relTable)] = relName + "#"
		}
	}
	return nil
}

func readForeignKeys(options *Options, meta *metadata.Service, db *sql.DB, relTable string, cardinality view.Cardinality) ([]sink.Key, error) {
	var fk []sink.Key

	var table, referencedTable string
	if strings.Title(string(cardinality)) == string(view.One) {
		table = options.Table
		referencedTable = relTable
	} else {
		table = relTable
		referencedTable = options.Table
	}

	err := meta.Info(context.Background(), db, info.KindForeignKeys, &fk, option.NewArgs("", options.Connector.DbName, table))
	if err != nil {
		return nil, err
	}

	var result = make([]sink.Key, 0)
	for i, candidate := range fk {
		if candidate.ReferenceTable == referencedTable {
			result = append(result, fk[i])
		}
	}

	return result, err
}
