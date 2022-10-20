package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/template/columns"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/toolbox/format"
)

func (s *Builder) buildAndAddView(ctx context.Context, viewConfig *viewConfig, selector *view.Config, indexNamespace bool, parameters ...*view.Parameter) (*view.View, error) {
	table := viewConfig.table
	connector, err := s.ConnectorRef(view.NotEmptyOf(table.Connector, s.options.Connector.DbName))
	if err != nil {
		return nil, err
	}

	db, err := s.DB(connector)
	if err != nil {
		return nil, err
	}

	if tableName, err := s.readColumnTypes(ctx, db, table); err != nil {
		fmt.Printf("[WARN] %v", fmt.Errorf("couldn't read table %v column types %w ", tableName, err).Error())
	}

	template, err := s.buildTemplate(ctx, viewConfig, parameters)
	if err != nil {
		return nil, err
	}

	relations, err := s.buildRelations(ctx, viewConfig, indexNamespace)
	if err != nil {
		return nil, err
	}

	columnsConfig, err := s.buildColumnsConfig(ctx, viewConfig)
	if err != nil {
		return nil, err
	}

	if viewConfig.viewType == view.SQLExecMode {
		selector = nil
	} else if table.TableMeta.Selector != nil {
		selector = table.TableMeta.Selector
	}

	tableName := view.NotEmptyOf(table.Name, table.OuterAlias)

	actualNamespaceSource := view.NotEmptyOf(table.NamespaceSource, table.OuterAlias)
	if selector != nil && selector.Namespace == "" && indexNamespace && actualNamespaceSource != "" {
		selector.Namespace = namespace(actualNamespaceSource)
	}

	cache, err := s.buildCache(viewConfig)
	if err != nil {
		return nil, err
	}

	result := &view.View{
		Name:          viewConfig.viewName,
		Table:         tableName,
		With:          relations,
		ColumnsConfig: columnsConfig,
		Selector:      selector,
		Template:      template,
		Connector:     connector,
		AllowNulls:    table.AllowNulls,
		SelfReference: viewConfig.table.TableMeta.Self,
		Cache:         cache,
		Mode:          viewConfig.viewType,
	}

	s.routeBuilder.AddViews(result)
	return result, nil
}

func (s *Builder) readColumnTypes(ctx context.Context, db *sql.DB, table *option.Table) (string, error) {
	if err := s.indexColumns(ctx, db, table.Name); err != nil {
		return table.Name, err
	}

	for _, v := range table.Deps {
		if columns.ContainsSelect(string(v)) {
			continue
		}

		if err := s.indexColumns(ctx, db, string(v)); err != nil {
			return string(v), err
		}
	}

	return "", nil
}

func (s *Builder) DB(connector *view.Connector) (*sql.DB, error) {
	connectorName := view.NotEmptyOf(connector.Name, connector.Ref)
	connector, ok := s.options.Lookup(connectorName)
	if !ok {
		return nil, fmt.Errorf("not found connector %v", connectorName)
	}

	return connector.DB()
}

func (s *Builder) ConnectorRef(name string) (*view.Connector, error) {
	connector, ok := s.options.Lookup(name)
	if !ok {
		return nil, fmt.Errorf("not found connector %v", name)
	}

	return &view.Connector{
		Reference: shared.Reference{Ref: connector.Name},
	}, nil
}

func (s *Builder) indexColumns(ctx context.Context, db *sql.DB, tableName string) error {
	if columns.ContainsSelect(tableName) {
		return nil
	}

	if s.tablesMeta.Indexed(tableName) {
		return nil
	}

	tableMeta := s.tablesMeta.TableMeta(tableName)

	ioColumns, err := columns.DetectColumns(context.Background(), db, tableName)
	if err != nil {
		return err
	}
	tableMeta.AddIoColumns(ioColumns)

	session, err := config.Session(ctx, db)
	if err != nil {
		return err
	}

	sinkColumns, err := config.Columns(ctx, session, db, tableName)
	if err != nil {
		return err
	}

	return tableMeta.AddSinkColumns(sinkColumns)
}

func stringsPtr(args ...string) *[]string {
	return &args
}

func boolPtr(b bool) *bool {
	return &b
}

func (s *Builder) buildRelations(ctx context.Context, config *viewConfig, indexNamespace bool) ([]*view.Relation, error) {
	result := make([]*view.Relation, 0, len(config.relations))
	for _, relation := range config.relations {
		relationName := relation.queryJoin.Alias
		relView, err := s.buildAndAddView(ctx, relation, &view.Config{
			Limit: 40,
			Constraints: &view.Constraints{
				Limit:  true,
				Offset: true,
				Page:   boolPtr(true),
			},
		}, indexNamespace)

		if err != nil {
			return nil, err
		}

		holderFormat, err := format.NewCase(view.DetectCase(relationName))
		if err != nil {
			return nil, err
		}

		var cardinality view.Cardinality
		//var column string
		//var ofColumn string
		//var field string
		if hasOneCardinalityPredicate(relation.queryJoin.On.X) {
			cardinality = view.One
			//column = relation.aKey.OwnerKey
			//ofColumn = relation.aKey.Key

		} else {
			cardinality = view.Many
		}

		result = append(result, &view.Relation{
			Name: config.viewName + "_" + relationName,
			Of: &view.ReferenceView{
				View: view.View{
					Reference: shared.Reference{Ref: relView.Name},
					Name:      relationName + "#",
				},
				Field:  relation.aKey.child.Field,
				Column: relation.aKey.child.Column,
			},
			Column:        relation.aKey.owner.Column,
			ColumnAlias:   relation.aKey.child.Alias,
			Holder:        holderFormat.Format(relationName, format.CaseUpperCamel),
			IncludeColumn: true,
			Cardinality:   cardinality,
		})
	}

	return result, nil
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

	if len(names) == 0 {
		names = append(names, xTable.Name)
	}

	return view.CaseFormat(view.DetectCase(names...))
}

func (s *Builder) buildColumnsConfig(ctx context.Context, config *viewConfig) (map[string]*view.ColumnConfig, error) {
	result := map[string]*view.ColumnConfig{}
	for _, column := range config.table.Inner {
		if column.Comments == "" {
			continue
		}

		columnConfig := &view.ColumnConfig{}
		if err := tryUnmarshalHint(column.Comments, columnConfig); err != nil {
			return nil, err
		}

		result[view.NotEmptyOf(column.Alias, column.Name)] = columnConfig
	}
	return result, nil
}

func (s *Builder) buildCache(viewConfig *viewConfig) (*view.Cache, error) {
	meta := viewConfig.table.TableMeta
	if meta.Cache == nil {
		return nil, nil
	}

	if meta.Cache.Warmup == nil {
		meta.Cache.Warmup = s.buildCacheWarmup(meta.Warmup, viewConfig.aKey)
	}

	return meta.Cache, nil
}
