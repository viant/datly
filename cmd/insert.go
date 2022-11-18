package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/router"
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

type (
	postRequestBodyData struct {
		typeDef      *view.Definition
		meta         *typeMeta
		actualFields []*view.Field
		bodyHolder   string
		paramName    string
	}

	typeMeta struct {
		index map[string]int
		metas []*fieldMeta
	}

	fieldMeta struct {
		primaryKey    bool
		autoincrement bool
		foreignKey    bool
		generator     string
		columnName    string
		fieldName     string
		columnCase    format.Case
	}
)

func (s *Builder) preparePostRule(ctx context.Context, sourceSQL []byte) (string, error) {
	hint, SQL := s.extractRouteSettings(sourceSQL)

	routeOption := &option.RouteConfig{}
	if err := tryUnmarshalHint(hint, routeOption); err != nil {
		return "", err
	}

	configurer, err := NewConfigProviderReader("", SQL, s.routeBuilder.option, map[string]*sanitize.ParameterHint{}, router.ReaderServiceType, map[string]interface{}{})
	if err != nil {
		return "", err
	}

	config := configurer.ViewConfig()

	connectorRef, err := s.ConnectorRef(view.FirstNotEmpty(config.expandedTable.Connector, s.options.Connector.DbName))
	if err != nil {
		return "", err
	}

	db, err := s.DB(connectorRef)
	if err != nil {
		return "", err
	}

	template, err := s.detectTypeAndBuildPostSQL(ctx, config, db, routeOption)
	if err != nil {
		return "", err
	}

	if _, err = s.uploadSQL(folderSQL, s.unique(config.fileName, s.fileNames, false), template, false); err != nil {
		return "", nil
	}

	return template, nil
}

func (s *Builder) extractRouteSettings(sourceSQL []byte) (string, string) {
	hint := sanitize.ExtractHint(string(sourceSQL))
	SQL := strings.Replace(string(sourceSQL), hint, "", 1)
	return hint, SQL
}

func (s *Builder) detectTypeAndBuildPostSQL(ctx context.Context, aViewConfig *viewConfig, db *sql.DB, routeOption *option.RouteConfig) (string, error) {
	tableName := aViewConfig.expandedTable.Name
	parameterType, err := s.detectInputType(ctx, db, tableName, aViewConfig)
	if err != nil {
		return "", err
	}

	return s.buildInsertSQL(tableName, parameterType, aViewConfig, routeOption)
}

func (s *Builder) uploadGoType(name string, rType reflect.Type) error {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	fileContent := xreflect.GenerateStruct(name, rType)
	if _, err := s.uploadGo(folderSQL, name, fileContent, false); err != nil {
		return err
	}

	return nil
}

func (s *Builder) detectInputType(ctx context.Context, db *sql.DB, tableName string, config *viewConfig) (*postRequestBodyData, error) {
	columns, err := s.readSinkColumns(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	foreignKeys, err := s.readForeignKeys(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	primaryKeys, err := s.readPrimaryKeys(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	return s.buildPostInputParameterType(columns, foreignKeys, primaryKeys, config)
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

func (s *Builder) buildPostInputParameterType(columns []sink.Column, foreignKeys []sink.Key, primaryKeys []sink.Key, config *viewConfig) (*postRequestBodyData, error) {
	fkIndex := s.indexKeys(foreignKeys)
	pkIndex := s.indexKeys(primaryKeys)

	var outputCase *format.Case
	if outputFormat := s.routeBuilder.option.CaseFormat; outputFormat != "" {
		newCase, err := format.NewCase(outputFormat)
		if err != nil {
			return nil, err
		}

		outputCase = &newCase
	}

	aFields := &typeMeta{index: map[string]int{}}
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
		Ptr:         true,
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
		fromName := meta.columnName
		if outputCase != nil {
			jsonTag = fmt.Sprintf(` json:"%v"`, meta.columnCase.Format(meta.columnName, *outputCase))
			fromName = meta.columnCase.Format(fromName, *outputCase)
		}

		aTag := fmt.Sprintf(`sqlx:"name=%v"%v`, column.Name, jsonTag)

		definition.Fields = append(definition.Fields, &view.Field{
			Name:   meta.fieldName,
			Tag:    aTag,
			Column: column.Name,
			Schema: &view.Schema{
				DataType: aType.String(),
			},
			FromName: fromName,
		})

		aFields.addMeta(meta)
	}

	holderName := ""
	paramName := name
	actualFields := definition.Fields
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

	return &postRequestBodyData{
		typeDef:      definition,
		meta:         aFields,
		actualFields: actualFields,
		paramName:    paramName,
		bodyHolder:   holderName,
	}, nil
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

	meta := &fieldMeta{
		columnCase: columnCase,
		columnName: column.Name,
		fieldName:  columnCase.Format(column.Name, format.CaseUpperCamel),
	}

	if _, ok := fkIndex[column.Name]; ok {
		meta.foreignKey = true
	}

	if _, ok := pkIndex[column.Name]; ok {
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
	f.index[meta.fieldName] = len(f.metas)
	f.metas = append(f.metas, meta)
}

func (s *Builder) indexKeys(primaryKeys []sink.Key) map[string]sink.Key {
	pkIndex := map[string]sink.Key{}
	for index, primaryKey := range primaryKeys {
		pkIndex[primaryKey.Name] = primaryKeys[index]
	}
	return pkIndex
}

func (s *Builder) buildInsertSQL(tableName string, typeDef *postRequestBodyData, config *viewConfig, routeOption *option.RouteConfig) (string, error) {
	sb := &strings.Builder{}
	paramType, err := s.buildRequestBodyPostParam(config, typeDef)
	if err != nil {
		return "", err
	}

	typeName := typeDef.typeDef.Name
	if err = s.uploadGoType(typeName, paramType); err != nil {
		return "", err
	}

	routeOption.Declare = map[string]string{}
	routeOption.TypeSrc = &option.TypeSrc{
		URL:   folderSQL,
		Types: []string{typeName},
	}

	routeOption.Declare[view.FirstNotEmpty(typeDef.bodyHolder, typeDef.paramName)] = typeName
	marshal, err := json.Marshal(routeOption)
	if err != nil {
		return "", err
	}

	if routeJSON := string(marshal); routeJSON != "{}" {
		sb.WriteString(fmt.Sprintf("/* %v */\n\n", routeJSON))
	}

	holderName := typeDef.paramName
	if typeDef.bodyHolder != "" {
		sb.WriteString(fmt.Sprintf("#set($%v = $Unsafe.%v.%v)", holderName, typeDef.bodyHolder, typeDef.bodyHolder))
		sb.WriteString("\n")
	}

	s.appendAllocation(sb, tableName, typeDef, holderName)

	recordName, multi := s.recordName(holderName, config)
	if multi {
		sb.WriteString("#foreach($")
		sb.WriteString(recordName)
		sb.WriteString(" in $")
		sb.WriteString(holderName)
		sb.WriteString(")")
		sb.WriteString("\n")
	}

	s.appendInsertStmt(sb, tableName, typeDef, recordName)

	if multi {
		sb.WriteString("\n#end")
	}

	return sb.String(), nil
}

func (s *Builder) appendInsertStmt(sb *strings.Builder, tableName string, typeDef *postRequestBodyData, name string) {
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" ( ")
	for i, field := range typeDef.actualFields {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(field.Column)
	}

	sb.WriteString(" ) VALUES ( ")
	for i, field := range typeDef.actualFields {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('$')
		sb.WriteString(name)
		sb.WriteByte('.')
		sb.WriteString(field.Name)
	}

	sb.WriteString("); ")
}

func (s *Builder) appendAllocation(sb *strings.Builder, tableName string, def *postRequestBodyData, holderName string) {
	for _, meta := range def.meta.metas {
		if !meta.autoincrement {
			continue
		}

		sb.WriteString(fmt.Sprintf(`$sequencer.Allocate("%v", $%v, "%v")`, tableName, holderName, meta.fieldName))
		sb.WriteString("\n")
	}
}

func (s *Builder) recordName(recordName string, config *viewConfig) (string, bool) {
	if !config.outputConfig.IsMany() {
		return recordName, false
	}

	return "rec" + strings.Title(recordName), true
}

func (s *Builder) buildRequestBodyPostParam(config *viewConfig, def *postRequestBodyData) (reflect.Type, error) {
	if err := def.typeDef.Init(context.Background(), map[string]reflect.Type{}); err != nil {
		return nil, err
	}

	return def.typeDef.Schema.Type(), nil
}
