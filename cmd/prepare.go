package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/router"
	json2 "github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/toolbox/format"
	"reflect"
	"sort"
	"strings"
)

type (
	stmtBuilder struct {
		indent  string
		sb      *strings.Builder
		typeDef *inputMetadata

		paramName string
		isMulti   bool
		wroteHint *bool
		paramKind string
	}

	viewFields []*view.Field
)

func (f viewFields) Len() int {
	return len(f)
}

func (f viewFields) Less(i, j int) bool {
	return !f[i].Ptr
}

func (f viewFields) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func newStmtBuilder(sb *strings.Builder, def *inputMetadata, paramKind string) *stmtBuilder {
	b := &stmtBuilder{
		sb:        sb,
		typeDef:   def,
		paramName: def.paramName,
		wroteHint: boolPtr(false),
		isMulti:   def.config.outputConfig.IsMany(),
		paramKind: paramKind,
	}

	return b
}

func (sb *stmtBuilder) appendColumnValues(accessor string, withHas bool) error {
	return sb.appendColumns(accessor, true, withHas, func(accessor string, field *view.Field) string {
		return fmt.Sprintf("$%v.%v", accessor, field.Name)
	}, nil)
}

func (sb *stmtBuilder) appendColumnNameValues(accessor string, withHas bool, fieldSkipper func(field *view.Field) bool) error {
	return sb.appendColumns(accessor, true, withHas, func(accessor string, field *view.Field) string {
		return fmt.Sprintf("%v = $%v.%v", field.Column, accessor, field.Name)
	}, fieldSkipper)
}

func (sb *stmtBuilder) appendColumnNames(accessor string, withHas bool) error {
	return sb.appendColumns(accessor, false, withHas, func(accessor string, field *view.Field) string {
		return field.Column
	}, nil)
}

func (sb *stmtBuilder) appendColumns(accessor string, withHint, withHas bool, content func(accessor string, field *view.Field) string, skipper func(field *view.Field) bool) error {
	var i = 0
	for _, field := range sb.typeDef.actualFields {
		if skipper != nil && skipper(field) {
			continue
		}

		if field.Ptr && withHas {
			sb.writeString(fmt.Sprintf("\n#if($%v.Has.%v == true)", accessor, field.Name))
		}

		if i != 0 {
			sb.writeString(" , ")
		}

		i++
		sb.writeString("\n")
		sb.writeString(content(accessor, field))

		if withHint {
			if err := sb.tryWriteParamHint(); err != nil {
				return err
			}
		}

		if field.Ptr && withHas {
			sb.writeString("\n#end")
		}
	}

	return nil
}

func (sb *stmtBuilder) paramHint(typeDef *inputMetadata) (string, error) {
	target := typeDef.bodyHolder

	paramConfig, err := json.Marshal(&option.ParameterConfig{
		Target:      &target,
		DataType:    "*" + typeDef.paramName,
		Cardinality: typeDef.typeDef.Cardinality,
		Kind:        sb.paramKind,
	})

	if err != nil {
		return "", err
	}

	return fmt.Sprintf(" /* %v */ ", string(paramConfig)), nil
}

func (sb *stmtBuilder) accessParam(parentRecord, record string, withUnsafe bool) string {
	result := parentRecord
	if result == "" {
		result = record
	} else {
		result += "." + record
	}

	if withUnsafe {
		result = "Unsafe." + result
	}

	return result
}

func (sb *stmtBuilder) tryWriteParamHint() error {
	if *sb.wroteHint {
		return nil
	}
	*sb.wroteHint = true
	paramHint, err := sb.paramHint(sb.typeDef)
	if err != nil {
		return err
	}

	sb.writeString(paramHint)
	return nil
}

func (sb *stmtBuilder) writeString(value string) {
	if sb.indent != "" {
		value = strings.ReplaceAll(value, "\n", "\n"+sb.indent)
	}

	sb.sb.WriteString(value)
}

func (sb *stmtBuilder) appendForEach(parentRecord, name string, withUnsafe bool) (parentName string, recordName string, err error) {
	sb.writeString("\n#foreach($")
	recName := "rec" + name
	sb.writeString(recName)
	sb.writeString(" in ")
	sb.writeString("$")
	sb.writeString(sb.accessParam(parentRecord, name, withUnsafe))
	if err := sb.tryWriteParamHint(); err != nil {
		return "", "", err
	}

	sb.writeString(")")

	return "", recName, err
}

func (sb *stmtBuilder) newRelation(rel *inputMetadata) *stmtBuilder {
	builder := newStmtBuilder(sb.sb, rel, "")
	builder.indent = sb.indent
	if builder.isMulti {
		builder.indent += "	"
	}

	builder.wroteHint = sb.wroteHint
	return builder
}

func (s *Builder) buildInputMetadata(ctx context.Context, sourceSQL []byte) (*option.RouteConfig, *viewConfig, *inputMetadata, error) {
	hint, SQL := s.extractRouteSettings(sourceSQL)

	routeOption := &option.RouteConfig{}
	if err := tryUnmarshalHint(hint, routeOption); err != nil {
		return nil, nil, nil, err
	}

	paramIndex := NewParametersIndex(routeOption, map[string]*sanitize.ParameterHint{})

	configurer, err := NewConfigProviderReader("", SQL, s.routeBuilder.option, router.ReaderServiceType, paramIndex)
	if err != nil {
		return nil, nil, nil, err
	}

	aConfig := configurer.ViewConfig()

	connectorRef, err := s.ConnectorRef(view.FirstNotEmpty(aConfig.expandedTable.Connector, s.options.Connector.DbName))
	if err != nil {
		return nil, nil, nil, err
	}

	db, err := s.DB(connectorRef)
	if err != nil {
		return nil, nil, nil, err
	}

	paramType, err := s.detectInputType(ctx, db, aConfig.expandedTable.Name, aConfig, "")
	if err != nil {
		return nil, nil, nil, err
	}
	return routeOption, aConfig, paramType, nil
}

func (s *Builder) detectInputType(ctx context.Context, db *sql.DB, tableName string, config *viewConfig, parentTable string) (*inputMetadata, error) {
	columns, err := s.readSinkColumns(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("not found table %v columns", tableName)
	}

	foreignKeys, err := s.readForeignKeys(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	primaryKeys, err := s.readPrimaryKeys(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	return s.buildPostInputParameterType(columns, foreignKeys, primaryKeys, config, db, tableName, parentTable)
}

func (s *Builder) readSinkColumns(ctx context.Context, db *sql.DB, tableName string) ([]sink.Column, error) {
	session, err := config.Session(ctx, db)
	if err != nil {
		return nil, err
	}

	sinkColumns, err := config.Columns(ctx, session, db, tableName)
	if err != nil {
		return nil, err
	}
	return sinkColumns, nil
}

func (s *Builder) readForeignKeys(ctx context.Context, db *sql.DB, tableName string) ([]sink.Key, error) {
	meta := metadata.New()
	var keys []sink.Key
	if err := meta.Info(ctx, db, info.KindForeignKeys, &keys); err != nil {
		return nil, err
	}

	return s.filterKeys(keys, tableName), nil
}

func (s *Builder) readPrimaryKeys(ctx context.Context, db *sql.DB, tableName string) ([]sink.Key, error) {
	meta := metadata.New()
	var keys []sink.Key
	if err := meta.Info(ctx, db, info.KindPrimaryKeys, &keys); err != nil {
		return nil, err
	}

	return s.filterKeys(keys, tableName), nil
}

func (s *Builder) buildPostInputParameterType(columns []sink.Column, foreignKeys []sink.Key, primaryKeys []sink.Key, config *viewConfig, db *sql.DB, table, parentTable string) (*inputMetadata, error) {
	fkIndex := s.indexKeys(foreignKeys)
	pkIndex := s.indexKeys(primaryKeys)

	typesMeta := &typeMeta{fieldIndex: map[string]int{}, columnIndex: map[string]int{}}
	name := config.expandedTable.HolderName
	detectCase, err := format.NewCase(view.DetectCase(name))
	if err != nil {
		return nil, err
	}

	if detectCase != format.CaseUpperCamel {
		name = detectCase.Format(name, format.CaseUpperCamel)
	}

	cardinality := view.One
	if config.outputConfig.IsMany() {
		cardinality = view.Many
	}
	definition := &view.Definition{
		Name:        name,
		Cardinality: cardinality,
	}

	exceptIndex := s.buildExceptIndex(config)
	includeIndex := s.buildIncludeIndex(config)

	for _, column := range columns {
		if s.shouldSkipColumn(exceptIndex, includeIndex, column) {
			continue
		}
		meta, err := s.buildFieldMeta(column, pkIndex, fkIndex)
		if err != nil {
			return nil, err
		}

		aType, err := view.GetOrParseType(map[string]reflect.Type{}, column.Type)
		if err != nil {
			return nil, err
		}

		tagContent := "name=" + column.Name
		if meta.primaryKey {
			tagContent += ",primaryKey=true"
		}

		if meta.autoincrement {
			tagContent += ",generator=autoincrement"
		} else if meta.generator != "" {
			tagContent += ",generator=" + meta.generator
		}
		var jsonTag string
		if !meta.required {
			jsonTag = ` json:",omitempty"`
		}

		sqlxTagContent := "name=" + column.Name
		aTag := fmt.Sprintf(`sqlx:"%v"%v`, sqlxTagContent, jsonTag)

		definition.Fields = append(definition.Fields, &view.Field{
			Name:   meta.fieldName,
			Tag:    aTag,
			Column: column.Name,
			Ptr:    !meta.required,
			Schema: &view.Schema{
				DataType: aType.String(),
			},
		})

		typesMeta.addMeta(meta)
	}

	holderName := ""
	paramName := name

	actualFields := make([]*view.Field, len(definition.Fields))
	copy(actualFields, definition.Fields)

	insertRelations, err := s.buildInsertRelations(config, db)
	if err != nil {
		return nil, err
	}

	for _, relation := range insertRelations {
		definition.Fields = append(definition.Fields, &view.Field{
			Name:        relation.paramName,
			Fields:      relation.typeDef.Fields,
			Tag:         fmt.Sprintf(`typeName:"%v" sqlx:"-"`, relation.paramName),
			Cardinality: relation.config.outputConfig.Cardinality,
			Ptr:         true,
		})
	}

	hasFieldName := "Has"
	hasField := &view.Field{
		Name: hasFieldName,
		Tag:  fmt.Sprintf(`%v:"true" typeName:"%v" json:"-" sqlx:"-"`, json2.IndexKey, definition.Name+"Has"),
		Ptr:  true,
	}

	for _, field := range actualFields {
		hasField.Fields = append(hasField.Fields, &view.Field{Name: field.Name, Schema: &view.Schema{DataType: "bool"}})
	}

	definition.Fields = append(definition.Fields, hasField)

	if !config.outputConfig.IsBasic() {
		holderName = config.outputConfig.Field()
		definition.Name = holderName
		definition.Fields = []*view.Field{
			{
				Name:        config.outputConfig.ResponseField,
				Fields:      definition.Fields,
				Cardinality: definition.Cardinality,
				Tag:         fmt.Sprintf(`typeName:"%v"`, paramName),
				Ptr:         true,
			},
		}
		definition.Cardinality = ""
	}

	sort.Sort(viewFields(actualFields))

	return &inputMetadata{
		typeDef:      definition,
		meta:         typesMeta,
		actualFields: actualFields,
		paramName:    paramName,
		bodyHolder:   holderName,
		relations:    insertRelations,
		fkIndex:      fkIndex,
		pkIndex:      pkIndex,
		table:        table,
		config:       config,
	}, nil
}

func (s *Builder) shouldFilterColumnByMeta(parentTable string, fkIndex map[string]sink.Key, fieldMeta *fieldMeta) bool {
	if fieldMeta.fkKey == nil {
		return false
	}

	if parentColumn, ok := fkIndex[strings.ToLower(fieldMeta.columnName)]; ok {
		return parentColumn.ReferenceTable != parentTable
	}

	return true
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

func (s *Builder) buildInsertRelations(config *viewConfig, db *sql.DB) ([]*inputMetadata, error) {
	var relations []*inputMetadata
	for _, relation := range config.relations {
		relationConfig, err := s.detectInputType(context.TODO(), db, relation.expandedTable.Name, relation, config.expandedTable.Name)
		if err != nil {
			return nil, err
		}

		relations = append(relations, relationConfig)
	}

	return relations, nil
}

func (s *Builder) shouldSkipColumn(exceptIndex map[string]bool, includeIndex map[string]bool, column sink.Column) bool {
	columnName := strings.ToLower(column.Name)
	if len(exceptIndex) > 0 {
		return exceptIndex[columnName]
	}

	if len(includeIndex) > 0 {
		return !includeIndex[columnName]
	}

	return false
}

func (s *Builder) buildExceptIndex(config *viewConfig) map[string]bool {
	exceptIndex := map[string]bool{}
	for _, column := range config.expandedTable.Columns {
		for _, except := range column.Except {
			exceptIndex[strings.ToLower(except)] = true
		}
	}

	return exceptIndex
}

func (s *Builder) buildIncludeIndex(config *viewConfig) map[string]bool {
	includeIndex := map[string]bool{}
	for _, column := range config.expandedTable.Inner {
		if column.Name == "*" {
			return includeIndex
		}
	}

	for _, column := range config.expandedTable.Inner {
		includeIndex[strings.ToLower(column.Name)] = true
	}
	return includeIndex
}

func (s *Builder) buildFieldMeta(column sink.Column, pkIndex map[string]sink.Key, fkIndex map[string]sink.Key) (*fieldMeta, error) {
	columnCase, err := format.NewCase(view.DetectCase(column.Name))
	if err != nil {
		return nil, err
	}

	isRequired := strings.ToLower(column.Nullable) != "yes"
	meta := &fieldMeta{
		columnCase: columnCase,
		columnName: column.Name,
		fieldName:  columnCase.Format(column.Name, format.CaseUpperCamel),
		required:   isRequired,
	}

	if fkKey, ok := fkIndex[strings.ToLower(column.Name)]; ok {
		meta.fkKey = &fkKey
	}

	if _, ok := pkIndex[strings.ToLower(column.Name)]; ok {
		meta.primaryKey = true
	}

	if column.Default != nil && *column.Default != "" {
		if s.containsAutoincrement(*column.Default) {
			meta.autoincrement = true
		}
		meta.generator = *column.Default
	}

	meta.autoincrement = meta.autoincrement || (column.IsAutoincrement != nil && *column.IsAutoincrement)
	if meta.autoincrement && meta.generator == "" {
		meta.generator = "autoincrement"
	}

	return meta, nil
}

func (s *Builder) containsAutoincrement(text string) bool {
	textLower := strings.ToLower(text)
	return strings.Contains(textLower, "autoincrement") || strings.Contains(textLower, "auto_increment")
}

func (f *typeMeta) addMeta(meta *fieldMeta) {
	f.fieldIndex[strings.ToLower(meta.fieldName)] = len(f.metas)
	f.columnIndex[strings.ToLower(meta.columnName)] = len(f.metas)
	f.metas = append(f.metas, meta)
}

func (f *typeMeta) metaByColName(column string) (*fieldMeta, bool) {
	i, ok := f.columnIndex[strings.ToLower(column)]
	if !ok {
		return nil, false
	}
	return f.metas[i], true
}

func (s *Builder) indexKeys(primaryKeys []sink.Key) map[string]sink.Key {
	pkIndex := map[string]sink.Key{}
	for index, primaryKey := range primaryKeys {
		pkIndex[strings.ToLower(primaryKey.Column)] = primaryKeys[index]
	}
	return pkIndex
}
