package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/toolbox/format"
	"reflect"
	"strings"
)

func (s *Builder) preparePostRule(ctx context.Context, aViewConfig *viewConfig) (*view.View, error) {
	connectorRef, err := s.ConnectorRef(view.FirstNotEmpty(aViewConfig.expandedTable.Connector, s.options.Connector.DbName))
	if err != nil {
		return nil, err
	}

	db, err := s.DB(connectorRef)
	if err != nil {
		return nil, err
	}

	template, err := s.buildPostTemplate(ctx, aViewConfig, db)
	if err != nil {
		return nil, err
	}

	return &view.View{
		Name:      aViewConfig.viewName,
		Table:     aViewConfig.expandedTable.Name,
		Connector: connectorRef,
		Template:  template,
	}, nil
}

func (s *Builder) buildPostTemplate(ctx context.Context, aViewConfig *viewConfig, db *sql.DB) (*view.Template, error) {
	tableName := aViewConfig.expandedTable.Name
	inputType, err := s.detectInputType(ctx, db, tableName, aViewConfig.expandedTable.HolderName)
	if err != nil {
		return nil, err
	}

	template := &view.Template{
		Source: s.buildInsertSQL(tableName, inputType),
		Parameters: []*view.Parameter{
			{
				Name:     inputType.Name,
				Required: boolPtr(true),
				In: &view.Location{
					Kind: view.RequestBodyKind,
				},
				Schema: s.NewSchema(inputType.Name, string(view.One)),
			},
		},
	}

	s.addTypeDef(inputType)
	return template, nil
}

func (s *Builder) detectInputType(ctx context.Context, db *sql.DB, tableName string, holderName string) (*view.Definition, error) {
	columns, err := s.readSinkColumns(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	keys, err := s.readKeys(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	return s.buildPostInputType(columns, keys, holderName)
}

func (s *Builder) readKeys(ctx context.Context, db *sql.DB, tableName string) ([]sink.Key, error) {
	foreignKeys, err := s.readForeignKeys(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	primaryKeys, err := s.readPrimaryKeys(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	foreignKeys = append(foreignKeys, primaryKeys...)
	return foreignKeys, nil
}

func (s *Builder) readForeignKeys(ctx context.Context, db *sql.DB, tableName string) ([]sink.Key, error) {
	meta := metadata.New()
	var keys []sink.Key
	if err := meta.Info(ctx, db, info.KindForeignKeys, &keys); err != nil {
		return nil, err
	}

	return s.filterKeys(keys, tableName), nil
}

func (s *Builder) filterKeys(keys []sink.Key, tableName string) []sink.Key {
	var tableKeys []sink.Key
	for i, aKey := range keys {
		if aKey.Table == tableName {
			tableKeys = append(tableKeys, keys[i])
		}
	}
	return tableKeys
}

func (s *Builder) readPrimaryKeys(ctx context.Context, db *sql.DB, tableName string) ([]sink.Key, error) {
	meta := metadata.New()
	var keys []sink.Key
	if err := meta.Info(ctx, db, info.KindPrimaryKeys, &keys); err != nil {
		return nil, err
	}

	return s.filterKeys(keys, tableName), nil
}

func (s *Builder) buildPostInputType(columns []sink.Column, keys []sink.Key, name string) (*view.Definition, error) {
	keysIndex := map[string]bool{}
	for _, aKey := range keys {
		keysIndex[aKey.Column] = true
	}

	definition := &view.Definition{
		Name: name,
	}

	for _, column := range columns {
		if keysIndex[column.Name] {
			continue
		}

		if column.Default != nil && *column.Default != "" {
			continue
		}

		aType, err := view.GetOrParseType(map[string]reflect.Type{}, column.Type)
		if err != nil {
			return nil, err
		}

		columnCase, err := format.NewCase(view.DetectCase(column.Name))
		if err != nil {
			return nil, err
		}

		aTag := fmt.Sprintf(`sqlx:"name=%v"`, column.Name)

		definition.Fields = append(definition.Fields, &view.Field{
			Name:   columnCase.Format(column.Name, format.CaseUpperCamel),
			Tag:    aTag,
			Column: column.Name,
			Schema: &view.Schema{
				DataType: aType.String(),
			},
		})
	}

	return definition, nil
}

func (s *Builder) buildInsertSQL(name string, inputType *view.Definition) string {
	sb := &strings.Builder{}
	sb.WriteString("INSERT INTO ")
	sb.WriteString(name)
	sb.WriteString(" ( ")
	for i, field := range inputType.Fields {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(field.Column)
	}

	sb.WriteString(" ) VALUES ( ")
	for i, field := range inputType.Fields {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("$" + field.Name)
	}
	sb.WriteString(" ) ")
	return sb.String()
}
