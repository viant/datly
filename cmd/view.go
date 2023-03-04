package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/template/columns"
	"github.com/viant/datly/utils"
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/toolbox/format"
)

func (s *Builder) buildAndAddViewWithLog(ctx context.Context, viewConfig *viewConfig, selector *view.Config, indexNamespace bool, parameters ...*view.Parameter) (*view.View, error) {
	fmt.Printf("[INFO] building view %v\n", viewConfig.viewName)
	aView, err := s.buildAndAddView(ctx, viewConfig, selector, indexNamespace, parameters)
	if err != nil {
		fmt.Printf("[ERROR] couldn't build view %v due to the %v\n", viewConfig.viewName, err.Error())
	} else {
		fmt.Printf("[INFO] built view %v\n", viewConfig.viewName)
	}

	return aView, err
}

func (s *Builder) buildAndAddView(ctx context.Context, viewConfig *viewConfig, selector *view.Config, indexNamespace bool, parameters []*view.Parameter) (*view.View, error) {
	table := viewConfig.unexpandedTable
	viewName := s.viewNames.unique(viewConfig.viewName)
	connector, err := s.ConnectorRef(view.FirstNotEmpty(table.Connector, s.options.Connector.DbName))
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
	} else if table.ViewConfig.Selector != nil {
		selector = table.ViewConfig.Selector
	}

	tableName := view.FirstNotEmpty(table.Name, table.HolderName)

	actualNamespaceSource := view.FirstNotEmpty(table.NamespaceSource, table.HolderName)
	if selector != nil && selector.Namespace == "" && indexNamespace && actualNamespaceSource != "" {
		selector.Namespace = namespace(actualNamespaceSource)
	}

	cache, err := s.buildCache(viewConfig)
	if err != nil {
		return nil, err
	}

	result := &view.View{
		Name:          viewName,
		Table:         tableName,
		With:          relations,
		ColumnsConfig: columnsConfig,
		Selector:      selector,
		Template:      template,
		Connector:     connector,
		AllowNulls:    table.AllowNulls,
		SelfReference: viewConfig.unexpandedTable.ViewConfig.Self,
		Cache:         cache,
		Mode:          viewConfig.viewType,
		TableBatches:  viewConfig.batchEnabled,
	}

	s.routeBuilder.AddViews(result)

	return result, nil
}

func (s *Builder) readColumnTypes(ctx context.Context, db *sql.DB, table *Table) (string, error) {
	if err := s.indexColumnsWithLog(ctx, db, table); err != nil {
		return table.Name, err
	}

	for _, v := range table.Deps {
		if columns.ContainsSelect(string(v)) {
			continue
		}

		if err := s.indexColumnsWithLog(ctx, db, &Table{Name: string(v)}); err != nil {
			return string(v), err
		}
	}

	return "", nil
}

func (s *Builder) DB(connector *view.Connector) (*sql.DB, error) {
	connectorName := view.FirstNotEmpty(connector.Name, connector.Ref)
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

func (s *Builder) indexColumnsWithLog(ctx context.Context, db *sql.DB, table *Table) error {
	tableName := table.Name

	if columns.ContainsSelect(tableName) || tableName == "" {
		return nil
	}

	if s.tablesMeta.Indexed(tableName) {
		return nil
	}

	fmt.Printf("[INFO] reading %v table column types\n", tableName)
	err := s.indexColumns(ctx, db, table)
	if err != nil {
		fmt.Printf("[WARN] couldn't read table %v column types\n", tableName)
	} else {
		fmt.Printf("[INFO] finished reading table %v column types\n", tableName)
	}

	return err
}

func (s *Builder) indexColumns(ctx context.Context, db *sql.DB, table *Table) error {
	tableName := table.Name
	tableMeta := s.tablesMeta.TableMeta(tableName)
	if table.SQL != "" {
		s.discoverySQLColumns(db, table, tableMeta)
	}
	ioColumns, err := columns.DetectColumns(context.Background(), db, tableName)
	if err != nil {
		return err
	}
	tableMeta.AddIoColumns(ioColumns)
	sinkColumns, err := s.readSinkColumns(ctx, db, tableName)
	if err != nil {
		return err
	}
	return tableMeta.AddSinkColumns(sinkColumns)
}

func (s *Builder) discoverySQLColumns(db *sql.DB, table *Table, tableMeta *TableMeta) {
	SQL, err := normalizeSQL(table)
	if err != nil {
		return
	}

	ioColumns, err := columns.DetectColumns(context.Background(), db, SQL)
	tableMeta.AddIoColumns(ioColumns)
}

func normalizeSQL(table *Table) (string, error) {
	aQuery, err := sqlparser.ParseQuery(table.SQL)
	if err != nil {
		return "", err
	}
	aQuery.Limit = &expr.Literal{Value: "0", Kind: "int"}
	aQuery.Qualify = nil
	SQL := sqlparser.Stringify(aQuery)
	return SQL, nil
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
		relView, err := s.buildAndAddViewWithLog(ctx, relation, &view.Config{
			Limit: 40,
		}, indexNamespace)

		if err != nil {
			return nil, err
		}

		holderFormat, err := format.NewCase(utils.DetectCase(relationName))
		if err != nil {
			return nil, err
		}

		var cardinality view.Cardinality
		if hasOneCardinalityPredicate(relation.queryJoin.On.X) {
			cardinality = view.One
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
			Column:        relation.aKey.owner.Field,
			Field:         relation.aKey.owner.Field,
			ColumnAlias:   relation.aKey.child.Alias,
			Holder:        holderFormat.Format(relationName, format.CaseUpperCamel),
			IncludeColumn: true,
			Cardinality:   cardinality,
		})
	}

	return result, nil
}

func (s *Builder) buildColumnsConfig(ctx context.Context, config *viewConfig) (map[string]*view.ColumnConfig, error) {
	result := map[string]*view.ColumnConfig{}
	for _, column := range config.unexpandedTable.Inner {
		if column.Comments == "" {
			continue
		}

		columnConfig := &view.ColumnConfig{}
		if err := tryUnmarshalHint(column.Comments, columnConfig); err != nil {
			return nil, err
		}

		result[view.FirstNotEmpty(column.Alias, column.Name)] = columnConfig
	}
	return result, nil
}

func (s *Builder) buildCache(viewConfig *viewConfig) (*view.Cache, error) {
	meta := viewConfig.unexpandedTable.ViewConfig
	if meta.Cache == nil {
		return nil, nil
	}

	if meta.Cache.Warmup == nil {
		meta.Cache.Warmup = s.buildCacheWarmup(meta.Warmup, viewConfig.aKey)
	}

	return meta.Cache, nil
}
