package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/ast"
	"github.com/viant/datly/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	rdata "github.com/viant/toolbox/data"
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
		if err := updateViewSQL(options, join.Table, relView); err != nil {
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
		aView.With = append(aView.With, &view.Relation{
			Name: aView.Name + "_" + join.Table.Alias,
			Of: &view.ReferenceView{
				View:   view.View{Reference: shared.Reference{Ref: join.Table.Alias}, Name: join.Table.Alias + "#"},
				Column: join.Key,
				Field:  join.Field,
			},
			Cardinality: cardinality,
			Column:      join.OwnerKey,
			ColumnAlias: join.KeyAlias,
			Holder:      strings.Title(join.Table.Alias),

			IncludeColumn: true,
		})
		viewRoute.Index.Namespace[namespace(join.Table.Alias)] = join.Table.Alias + "#"
	}
	return nil
}

func updateViewSQL(options *Options, table *Table, relView *view.View) error {
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

		relView.UseBindingPositions = boolPtr(!viewMeta.HasVeltySyntax)

		SQLURL := options.SQLURL(table.Alias + "_from")
		fs := afs.New()
		if err := fs.Upload(context.Background(), SQLURL, file.DefaultFileOsMode, strings.NewReader(SQL)); err != nil {
			return err
		}
		_, URI := url.Split(SQLURL, file.Scheme)
		relView.FromURL = URI

		if len(viewMeta.Parameters) > 0 || viewMeta.Source != "" {
			templateParameters := make([]*view.Parameter, len(viewMeta.Parameters))
			for i, parameter := range viewMeta.Parameters {
				templateParameters[i] = &view.Parameter{
					Name: parameter.Id,
					In: &view.Location{
						Kind: view.Kind(parameter.Kind),
						Name: parameter.Name,
					},
					Positions: parameter.Positions,
					Required:  boolPtr(parameter.Required),
					Schema:    &view.Schema{DataType: parameter.Type},
				}
			}

			relView.Template = &view.Template{
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
			relView.Template.SourceURL = URI
		}
	}
	return nil
}

func createAndEvalauteTemplate(meta *ast.ViewMeta) (string, error) {
	schemaFields := make([]reflect.StructField, len(meta.Parameters))
	presenceFields := make([]reflect.StructField, len(meta.Parameters))

	expandMap := rdata.Map{}
	for i, parameter := range meta.Parameters {
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
				fmt.Printf("skiping relation: %v due to %v", rel, err)
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
