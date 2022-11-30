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
	"time"
)

type (
	insertData struct {
		typeDef      *view.Definition
		meta         *typeMeta
		actualFields []*view.Field
		bodyHolder   string
		paramName    string
		relations    []*insertData
		fkIndex      map[string]sink.Key
		pkIndex      map[string]sink.Key
		table        string
		config       *viewConfig
	}

	typeMeta struct {
		fieldIndex  map[string]int
		columnIndex map[string]int
		metas       []*fieldMeta
	}

	fieldMeta struct {
		primaryKey    bool
		autoincrement bool
		generator     string
		columnName    string
		fieldName     string
		required      bool
		columnCase    format.Case
		fkKey         *sink.Key
	}

	insertStmtBuilder struct {
		indent    string
		sb        *strings.Builder
		typeDef   *insertData
		paramName string
		isMulti   bool

		parent    *insertStmtBuilder
		wroteHint *bool
	}
)

func (s *Builder) preparePostRule(ctx context.Context, sourceSQL []byte) (string, error) {
	hint, SQL := s.extractRouteSettings(sourceSQL)

	routeOption := &option.RouteConfig{}
	if err := tryUnmarshalHint(hint, routeOption); err != nil {
		return "", err
	}

	paramIndex := NewParametersIndex(routeOption, map[string]*sanitize.ParameterHint{})

	configurer, err := NewConfigProviderReader("", SQL, s.routeBuilder.option, router.ReaderServiceType, paramIndex)
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
	parameterType, err := s.detectInputType(ctx, db, tableName, aViewConfig, "")
	if err != nil {
		return "", err
	}

	return s.buildInsertSQL(parameterType, aViewConfig, routeOption)
}

var timeType = reflect.TypeOf(time.Time{})

func (s *Builder) uploadGoType(name string, rType reflect.Type) error {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	fileContent := xreflect.GenerateStruct(name, rType)
	if _, err := s.uploadGo(folderSQL, name, fileContent, false); err != nil {
		return err
	}

	sampleValue := reflect.New(rType)
	for i := 0; i < rType.NumField(); i++ {
		fieldType := rType.Field(i).Type
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}
		if fieldType.Kind() == reflect.Struct && fieldType != timeType {
			fieldValue := reflect.New(fieldType)
			sampleValue.Elem().Field(i).Elem().Set(fieldValue)
		}
	}
	sample := sampleValue.Elem().Interface()
	if data, err := json.Marshal(sample); err == nil {
		s.uploadFile(folderSQL, name+"Post", string(data), false, ".json")
	}

	return nil
}

func (s *Builder) detectInputType(ctx context.Context, db *sql.DB, tableName string, config *viewConfig, parentTable string) (*insertData, error) {
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

	return s.buildPostInputParameterType(columns, foreignKeys, primaryKeys, config, db, tableName, parentTable)
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

func (s *Builder) buildPostInputParameterType(columns []sink.Column, foreignKeys []sink.Key, primaryKeys []sink.Key, config *viewConfig, db *sql.DB, table, parentTable string) (*insertData, error) {
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

		//if s.shouldFilterColumnByMeta(parentTable, fkIndex, meta) {
		//	continue
		//}
		aType, err := view.GetOrParseType(map[string]reflect.Type{}, column.Type)
		if err != nil {
			return nil, err
		}
		if !meta.required {
			aType = reflect.PtrTo(aType)
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
		} else {
			if !meta.required {
				jsonTag = ` json:",omitempty"`
			}
		}

		sqlxTagContent := ""
		//if meta.fkKey != nil {
		//	//TODO: introduce fk sqlx tag
		//	sqlxTagContent = `-`
		//} else {
		sqlxTagContent = "name=" + column.Name
		//		}

		aTag := fmt.Sprintf(`sqlx:"%v"%v`, sqlxTagContent, jsonTag)

		definition.Fields = append(definition.Fields, &view.Field{
			Name:   meta.fieldName,
			Tag:    aTag,
			Column: column.Name,
			Schema: &view.Schema{
				DataType: aType.String(),
			},
			FromName: fromName,
		})

		typesMeta.addMeta(meta)
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

	return &insertData{
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

func (s *Builder) buildInsertRelations(config *viewConfig, db *sql.DB) ([]*insertData, error) {
	var relations []*insertData
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

func (s *Builder) buildInsertSQL(typeDef *insertData, config *viewConfig, routeOption *option.RouteConfig) (string, error) {
	sb := &strings.Builder{}
	typeName := typeDef.typeDef.Name

	paramType, err := s.buildRequestBodyPostParam(config, typeDef)
	if err != nil {
		return "", err
	}

	if err = s.uploadGoType(typeName, paramType); err != nil {
		return "", err
	}

	if err = s.appendPostRouteOption(routeOption, typeName, typeDef, sb); err != nil {
		return "", err
	}

	builder := newInsertStmtBuilder(sb, typeDef)
	builder.appendAllocation(typeDef, "", typeDef.paramName)

	return builder.build("", true)
}

func (s *Builder) appendPostRouteOption(routeOption *option.RouteConfig, typeName string, typeDef *insertData, sb *strings.Builder) error {
	requiredTypes := []string{"*" + typeDef.paramName}
	routeOption.RequestBody = &option.BodyConfig{
		ReturnAsResponse: true,
		DataType:         typeDef.bodyHolder,
	}

	if typeDef.bodyHolder != "" {
		requiredTypes = append(requiredTypes, "*"+typeDef.bodyHolder)
	}

	routeOption.Declare = map[string]string{}
	routeOption.TypeSrc = &option.TypeSrcConfig{
		URL:   folderSQL,
		Types: requiredTypes,
	}

	routeOption.Declare[view.FirstNotEmpty(typeDef.bodyHolder, typeDef.paramName)] = typeName
	marshal, err := json.Marshal(routeOption)
	if err != nil {
		return err
	}

	if routeJSON := string(marshal); routeJSON != "{}" {
		sb.WriteString(fmt.Sprintf("/* %v */\n\n", routeJSON))
	}
	return nil
}

func (isb *insertStmtBuilder) appendAllocation(def *insertData, path, holderName string) {
	for _, meta := range def.meta.metas {
		if !meta.autoincrement {
			continue
		}

		isb.writeString(fmt.Sprintf(`$sequencer.Allocate("%v", $%v, "%v")`, def.table, holderName, path+meta.fieldName))
		isb.writeString("\n")
	}

	for _, relation := range def.relations {
		actualPath := path
		if actualPath == "" {
			actualPath = relation.paramName + "/"
		} else {
			actualPath += relation.paramName + "/"
		}
		isb.appendAllocation(relation, actualPath, holderName)
	}
}

func (s *Builder) recordName(recordName string, config *viewConfig) (string, bool) {
	if !config.outputConfig.IsMany() {
		return recordName, false
	}

	return "rec" + strings.Title(recordName), true
}

func (s *Builder) buildRequestBodyPostParam(config *viewConfig, def *insertData) (reflect.Type, error) {
	if err := def.typeDef.Init(context.Background(), map[string]reflect.Type{}); err != nil {
		return nil, err
	}

	return def.typeDef.Schema.Type(), nil
}

func newInsertStmtBuilder(sb *strings.Builder, def *insertData) *insertStmtBuilder {
	return &insertStmtBuilder{
		sb:        sb,
		typeDef:   def,
		paramName: def.paramName,
		wroteHint: boolPtr(false),
		isMulti:   def.config.outputConfig.IsMany(),
	}
}

func (isb *insertStmtBuilder) build(parentRecord string, withUnsafe bool) (string, error) {
	name := isb.paramName
	indirectParent := parentRecord

	if isb.isMulti {
		isb.writeString("\n#foreach($")
		recName := "rec" + name
		isb.writeString(recName)
		isb.writeString(" in ")
		isb.writeString("$")
		isb.writeString(isb.accessParam(parentRecord, name, withUnsafe))
		if err := isb.tryWriteParamHint(); err != nil {
			return "", err
		}

		isb.writeString(")")
		name = recName
		indirectParent = ""
	}

	if isb.parent != nil {
		for _, meta := range isb.typeDef.meta.metas {
			if meta.fkKey == nil {
				continue
			}

			if meta.fkKey.ReferenceTable != isb.parent.typeDef.table {
				continue
			}

			refMeta, ok := isb.parent.typeDef.meta.metaByColName(meta.fkKey.ReferenceColumn)
			if !ok {
				continue
			}

			isb.writeString(fmt.Sprintf("\n#set($%v.%v = $%v)", isb.accessParam(indirectParent, name, withUnsafe && !isb.isMulti), meta.fieldName, isb.accessParam(parentRecord, refMeta.fieldName, withUnsafe)))
		}
	}

	isb.writeString("\nINSERT INTO ")
	isb.writeString(isb.typeDef.table)
	isb.writeString(" (\n")

	for i, field := range isb.typeDef.actualFields {
		if i != 0 {
			isb.writeString(",\n")
		}
		isb.writeString(field.Column)
	}

	isb.writeString("\n) VALUES (\n")
	for i, field := range isb.typeDef.actualFields {
		if i != 0 {
			isb.writeString(",\n")
		}
		isb.writeString("$")
		isb.writeString(isb.accessParam(indirectParent, name, false))
		isb.writeString(".")
		isb.writeString(field.Name)
		if err := isb.tryWriteParamHint(); err != nil {
			return "", err
		}
	}
	isb.writeString("\n);\n")

	for _, rel := range isb.typeDef.relations {
		_, err := isb.newRelation(rel).build(name, !isb.isMulti && withUnsafe)
		if err != nil {
			return "", err
		}
	}

	if isb.isMulti {
		isb.writeString("\n#end")
	}
	return isb.sb.String(), nil
}

func (isb *insertStmtBuilder) writeString(value string) {
	if isb.indent != "" {
		value = strings.ReplaceAll(value, "\n", "\n"+isb.indent)
	}

	isb.sb.WriteString(value)
}

func (isb *insertStmtBuilder) paramHint(typeDef *insertData) (string, error) {
	target := typeDef.bodyHolder

	paramConfig, err := json.Marshal(&option.ParameterConfig{
		Target:      &target,
		DataType:    typeDef.paramName,
		Cardinality: typeDef.typeDef.Cardinality,
	})

	if err != nil {
		return "", err
	}

	return fmt.Sprintf(" /* %v */ ", string(paramConfig)), nil
}

func (isb *insertStmtBuilder) newRelation(rel *insertData) *insertStmtBuilder {
	builder := newInsertStmtBuilder(isb.sb, rel)
	builder.indent = isb.indent
	if builder.isMulti {
		builder.indent += "	"
	}
	builder.parent = isb
	builder.wroteHint = isb.wroteHint
	return builder
}

func (isb *insertStmtBuilder) tryWriteParamHint() error {
	if *isb.wroteHint {
		return nil
	}
	*isb.wroteHint = true
	paramHint, err := isb.paramHint(isb.typeDef)
	if err != nil {
		return err
	}

	isb.writeString(paramHint)
	return nil
}

func (isb *insertStmtBuilder) accessParam(parentRecord, record string, withUnsafe bool) string {
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
