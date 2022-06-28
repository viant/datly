package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
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
		updateViewSQL(options, join.Table, relView)

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

func updateViewSQL(options *Options, table *Table, relView *view.View) {
	if SQL := table.SQL; SQL != "" {
		SQLURL := options.SQLURL(table.Alias)
		fs := afs.New()
		fs.Upload(context.Background(), SQLURL, file.DefaultFileOsMode, strings.NewReader(SQL))
		_, URI := url.Split(SQLURL, file.Scheme)
		relView.FromURL = URI
	}
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
