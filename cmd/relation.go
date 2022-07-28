package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/datly/cmd/ast"
	"github.com/viant/datly/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/ast/parser"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"github.com/viant/toolbox/format"
	"strings"
)

func lookupView(resource *view.Resource, name string) *view.View {
	for _, candidate := range resource.Views {
		if candidate.Name == name {
			return candidate
		}
		if candidate.Table == name {
			return candidate
		}
	}
	return nil
}

func (s *serverBuilder) buildXRelations(ctx context.Context, viewRoute *router.Route, xTable *Table) error {
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

		if _, err := s.addViewConn(s.options.Connector.DbName, relView); err != nil {
			return err
		}

		if err := s.updateView(ctx, join.Table, relView); err != nil {
			return err
		}

		s.route.Resource.AddViews(relView)
		var cardinality = view.Many
		if join.ToOne {
			cardinality = view.One
		}

		ownerView := lookupView(s.route.Resource, join.Owner.Ref)
		if ownerView == nil {
			return fmt.Errorf("failed to lookup view: %v", join.Owner.Name)
		}

		columnNames := make([]string, 0)
		for _, column := range xTable.Inner {
			columnName := column.Alias
			if columnName == "" {
				columnName = column.Name
			}

			columnNames = append(columnNames, columnName)
		}

		newCase, err := format.NewCase(view.DetectCase(columnNames...))
		if err != nil {
			return err
		}

		withView := &view.Relation{
			Name: ownerView.Name + "_" + join.Table.Alias,
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
			relView.Connector = connectorRef(join.Connector)
		}
		relView.Cache = join.Cache
		ownerView.With = append(ownerView.With, withView)

		viewRoute.Index.Namespace[namespace(join.Table.Alias)] = join.Table.Alias + "#"

		if len(join.Table.Joins) > 0 {
			if err := s.buildXRelations(ctx, viewRoute, join.Table); err != nil {
				return err
			}
		}

	}
	return nil
}

func connectorRef(name string) *view.Connector {
	return &view.Connector{Reference: shared.Reference{Ref: name}}
}

func (s *serverBuilder) updateView(ctx context.Context, table *Table, aView *view.View) error {
	if table == nil {
		return nil
	}

	s.logger.Write([]byte(fmt.Sprintf("Discovering  %v metadata ...\n", aView.Name)))
	s.updateTableColumnTypes(ctx, table)
	s.updateParameterTypes(table)
	if err := s.updateViewMeta(table, aView); err != nil {
		return err
	}

	if err := s.updateColumnsConfig(table, aView); err != nil {
		return err
	}

	if table.ViewMeta == nil {
		return nil
	}

	if err := s.buildSQLSource(aView, table); err != nil {
		return err
	}
	return nil
}

func (s *serverBuilder) updateViewMeta(table *Table, aView *view.View) error {
	viewHint := strings.TrimSpace(strings.Trim(table.ViewHint, "/**/"))
	if viewHint == "" {
		return nil
	}

	tableMeta := &TableMeta{
		Selector: aView.Selector,
	}

	if err := json.Unmarshal([]byte(viewHint), tableMeta); err != nil {
		return err
	}

	if tableMeta.Cache != nil {
		aView.Cache = tableMeta.Cache
	}

	if tableMeta.Connector != "" {
		if _, err := s.addViewConn(tableMeta.Connector, aView); err != nil {
			return err
		}
	}

	return nil
}

func (s *serverBuilder) buildSQLSource(aView *view.View, table *Table) error {
	templateParams := make([]*view.Parameter, len(table.ViewMeta.Parameters))
	for i, param := range table.ViewMeta.Parameters {
		templateParams[i] = convertMetaParameter(param)
	}

	template := &view.Template{
		Parameters: templateParams,
	}

	aView.Template = template

	if err := s.updateTemplateSource(template, table); err != nil {
		return err
	}

	if err := s.updateViewSource(aView, table); err != nil {
		return err
	}

	return nil
}

func convertMetaParameter(param *ast.Parameter) *view.Parameter {
	return &view.Parameter{
		Name:   param.Id,
		Schema: &view.Schema{DataType: param.DataType},
		In: &view.Location{
			Kind: view.Kind(param.Kind),
			Name: param.Name,
		},
		Required: param.Required,
	}
}

func (s *serverBuilder) updateViewSource(aView *view.View, table *Table) error {
	if table.ViewMeta.From == "" {
		return nil
	}
	URI, err := s.uploadSQL(table.Alias, table.ViewMeta.From)
	if err != nil {
		return err
	}

	aView.FromURL = URI
	return nil
}

func (s *serverBuilder) updateTemplateSource(template *view.Template, table *Table) error {
	if table.ViewMeta.Source == "" {
		return nil
	}

	URI, err := s.uploadSQL(table.Alias, table.ViewMeta.Source)
	if err != nil {
		return err
	}

	template.SourceURL = URI
	return nil
}

func (s *serverBuilder) uploadSQL(fileName string, SQL string) (string, error) {
	sourceURL := s.options.SQLURL(fileName, true)
	fs := afs.New()
	if err := fs.Upload(context.Background(), sourceURL, file.DefaultFileOsMode, strings.NewReader(SQL)); err != nil {
		return "", err
	}

	skipped := 0
	anIndex := strings.LastIndexFunc(sourceURL, func(r rune) bool {
		if r == '/' {
			skipped++
		}

		if skipped == 2 {
			return true
		}
		return false
	})
	sourceURL = sourceURL[anIndex+1:]
	return sourceURL, nil
}

func (s *serverBuilder) updateColumnsConfig(table *Table, aView *view.View) error {
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

func (s *serverBuilder) buildRelations(ctx context.Context, meta *metadata.Service, aView *view.View, viewRoute *router.Route) error {
	pk := []sink.Key{}
	conn, ok := s.connectors[s.options.DbName]
	if !ok {
		return nil
	}

	db, err := conn.DB(ctx)
	if err != nil {
		return err
	}

	if err := meta.Info(context.Background(), db, info.KindPrimaryKeys, &pk, option.NewArgs("", s.options.Connector.DbName, s.options.Table)); err == nil && len(pk) > 0 {
		for _, rel := range s.options.Relations {
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

			fk, err := readForeignKeys(s.options, meta, db, relTable, relationCardinality)
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

			s.route.Resource.AddViews(relView)
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
