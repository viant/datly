package cmd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/option"
	dConfig "github.com/viant/datly/config"
	"github.com/viant/datly/router"
	json2 "github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
	goFormat "go/format"
	"path"
	"reflect"
	"sort"
	"strings"
)

const defaultIndent = "  "

type (
	stmtBuilder struct {
		indent  string
		sb      *strings.Builder
		typeDef *inputMetadata

		paramName string
		isMulti   bool
		paramKind string
		paramSQLs map[string]string
		withSQL   bool
	}

	paramAccessor struct {
		unsafeRecord string
		record       string
		parent       string
		unsafeParent string
		withUnsafe   bool
		name         interface{}
	}

	withSQL    bool
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

func newStmtBuilder(sb *strings.Builder, def *inputMetadata, options ...interface{}) *stmtBuilder {
	paramKind := string(view.KindRequestBody)
	var hintSQL bool

	for _, anOption := range options {
		switch actual := anOption.(type) {
		case view.Kind:
			paramKind = string(actual)
		case withSQL:
			hintSQL = bool(actual)
		}
	}
	b := &stmtBuilder{
		sb:        sb,
		typeDef:   def,
		paramName: def.paramName,
		isMulti:   def.config.outputConfig.IsMany(),
		paramKind: paramKind,
		paramSQLs: map[string]string{},
		withSQL:   hintSQL,
	}

	return b
}

func (b *stmtBuilder) appendColumnValues(accessor *paramAccessor, withHas bool) error {
	return b.appendColumns(accessor, withHas, func(accessor string, field *view.Field) string {
		return fmt.Sprintf("$%v.%v", accessor, field.Name)
	}, nil)
}

func (b *stmtBuilder) appendColumnNameValues(accessor *paramAccessor, withHas bool, fieldSkipper func(field *view.Field) bool) error {
	return b.appendColumns(accessor, withHas, func(accessor string, field *view.Field) string {
		return fmt.Sprintf("%v = $%v.%v", field.Column, accessor, field.Name)
	}, fieldSkipper)
}

func (b *stmtBuilder) appendColumnNames(accessor *paramAccessor, withHas bool) error {
	return b.appendColumns(accessor, withHas, func(accessor string, field *view.Field) string {
		return field.Column
	}, nil)
}

func (b *stmtBuilder) appendColumns(accessor *paramAccessor, withHas bool, content func(accessor string, field *view.Field) string, skipper func(field *view.Field) bool) error {
	var i = 0
	for index, field := range b.typeDef.actualFields {
		if skipper != nil && skipper(field) {
			continue
		}

		if field.Ptr && withHas {
			b.writeString(fmt.Sprintf("\n#if($%v.Has.%v == true)", accessor.unsafeRecord, field.Name))
		}

		if i == 0 {
			b.writeString("\n")
		} else {
			if len(b.typeDef.actualFields)-1 > index && b.typeDef.actualFields[index+1].Ptr && withHas {
				b.writeString("\n, ")
			} else {
				b.writeString(", \n")
			}
		}

		i++
		b.writeString(content(accessor.record, field))

		if field.Ptr && withHas {
			b.writeString("\n#end")
		}
	}

	return nil
}

func (b *stmtBuilder) paramHint(metadata *inputMetadata) (string, error) {
	hintBuilder := &strings.Builder{}
	hintBuilder.WriteString("<")
	if metadata.config.outputConfig.IsMany() {
		hintBuilder.WriteString("[]")
	}
	hintBuilder.WriteString("*")
	hintBuilder.WriteString(metadata.paramName)
	hintBuilder.WriteString(">(")
	hintBuilder.WriteString(b.paramKind)
	hintBuilder.WriteString("/")
	hintBuilder.WriteString(metadata.bodyHolder)
	hintBuilder.WriteString(")")

	return hintBuilder.String(), nil
}

func (b *stmtBuilder) accessParam(parentRecord, record string, withUnsafe bool) string {
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

func (b *stmtBuilder) writeString(value string) {
	if b.indent != "" {
		value = strings.ReplaceAll(value, "\n", "\n"+b.indent)
	}

	b.sb.WriteString(value)
}

func (b *stmtBuilder) appendForEachIfNeeded(parentRecord, name string, withUnsafe *bool, stack *Stack) (*paramAccessor, bool) {
	if !b.isMulti {
		return &paramAccessor{
			record:       b.accessParam(parentRecord, name, false),
			withUnsafe:   *withUnsafe,
			parent:       parentRecord,
			unsafeRecord: b.accessParam(parentRecord, name, *withUnsafe),
			unsafeParent: b.accessParam("", parentRecord, *withUnsafe),
			name:         name,
		}, false
	}

	b.writeString("\n#foreach($")
	recName := "rec" + name
	b.writeString(recName)
	b.writeString(" in ")
	b.writeString("$")
	b.writeString(b.accessParam(parentRecord, name, *withUnsafe))
	b.writeString(")")

	defer func() {
		*withUnsafe = false
	}()

	stack.Push(b)

	return &paramAccessor{
		unsafeRecord: recName,
		parent:       parentRecord,
		unsafeParent: b.accessParam("", parentRecord, *withUnsafe),
		record:       recName,
		withUnsafe:   false,
		name:         name,
	}, true
}

func (b *stmtBuilder) newRelation(rel *inputMetadata) *stmtBuilder {
	builder := newStmtBuilder(b.sb, rel, view.KindRequestBody)
	builder.indent = b.indent
	if builder.isMulti {
		builder.indent += defaultIndent
	}

	return builder
}

func (b *stmtBuilder) generateIndexes() ([]*indexChecker, error) {
	var checkers []*indexChecker
	err := b.iterateOverHints(b.typeDef, func(def *inputMetadata) error {
		index, ok := b.generateIndexIfNeeded(def)
		if ok {
			checkers = append(checkers, index)
		}

		return nil
	})

	return checkers, err
}

func (b *stmtBuilder) generateIndexIfNeeded(def *inputMetadata) (*indexChecker, bool) {
	for _, field := range def.actualFields {
		aMeta, ok := def.meta.metaByColName(field.Column)
		if !ok || !aMeta.primaryKey {
			continue
		}

		indexName, aFieldName := b.appendIndex(def, aMeta)

		return &indexChecker{
			indexName: indexName,
			field:     aFieldName,
			paramName: def.paramName,
		}, true
	}
	return nil, false
}

func (b *stmtBuilder) appendIndex(def *inputMetadata, aMeta *fieldMeta) (string, string) {
	indexName := fmt.Sprintf("%vIndex", def.sqlName)
	aFieldName := aMeta.fieldName

	b.sb.WriteString("\n")
	b.writeString(fmt.Sprintf("#set($%v = $%v.IndexBy(\"%v\"))", indexName, def.sqlName, aFieldName))
	return indexName, aFieldName
}

func (s *Builder) buildInputMetadata(ctx context.Context, sourceSQL []byte) (*option.RouteConfig, *viewConfig, *inputMetadata, error) {
	hint, SQL := s.extractRouteSettings(sourceSQL)

	routeOption := &option.RouteConfig{}
	if err := tryUnmarshalHint(hint, routeOption); err != nil {
		return nil, nil, nil, err
	}

	paramIndex := NewParametersIndex(routeOption, map[string]*sanitize.ParameterHint{})

	configurer, err := NewConfigProviderReader("", SQL, s.routeBuilder.option, router.ReaderServiceType, paramIndex, &s.options.Prepare, &s.options.Connector)
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

	paramType, err := s.detectInputType(ctx, db, aConfig.expandedTable.Name, aConfig, "", "/", "")
	if err != nil {
		return nil, nil, nil, err
	}
	return routeOption, aConfig, paramType, nil
}

func (s *Builder) detectInputType(ctx context.Context, db *sql.DB, tableName string, config *viewConfig, parentTable, path, actualHolder string) (*inputMetadata, error) {
	columns, err := s.readSinkColumns(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("not found table %v columns", tableName)
	}

	foreignKeys, err := readForeignKeys(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	primaryKeys, err := s.readPrimaryKeys(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	return s.buildPostInputParameterType(columns, foreignKeys, primaryKeys, config, db, tableName, parentTable, path, actualHolder)
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

func readForeignKeys(ctx context.Context, db *sql.DB, tableName string) ([]sink.Key, error) {
	meta := metadata.New()
	var keys []sink.Key
	if err := meta.Info(ctx, db, info.KindForeignKeys, &keys); err != nil {
		return nil, err
	}

	return filterKeys(keys, tableName), nil
}

func (s *Builder) readPrimaryKeys(ctx context.Context, db *sql.DB, tableName string) ([]sink.Key, error) {
	meta := metadata.New()
	var keys []sink.Key
	if err := meta.Info(ctx, db, info.KindPrimaryKeys, &keys); err != nil {
		return nil, err
	}

	return filterKeys(keys, tableName), nil
}

func (s *Builder) buildPostInputParameterType(columns []sink.Column, foreignKeys, primaryKeys []sink.Key, aConfig *viewConfig, db *sql.DB, table, parentTable, structPath, actualHolder string) (*inputMetadata, error) {
	if aConfig.outputConfig.Cardinality == "" {
		aConfig.outputConfig.Cardinality = view.Many
	}

	fkIndex := s.indexKeys(foreignKeys)
	pkIndex := s.indexKeys(primaryKeys)

	typesMeta := &typeMeta{fieldIndex: map[string]int{}, columnIndex: map[string]int{}}
	name := aConfig.expandedTable.HolderName
	detectCase, err := format.NewCase(view.DetectCase(name))
	if err != nil {
		return nil, err
	}

	if detectCase != format.CaseUpperCamel {
		name = detectCase.Format(name, format.CaseUpperCamel)
	}

	cardinality := view.One
	if aConfig.outputConfig.IsMany() {
		cardinality = view.Many
	}
	definition := &view.TypeDefinition{
		Name:        name,
		Cardinality: cardinality,
	}

	if actualHolder == "" {
		actualHolder = name
	}

	exceptIndex := s.buildExceptIndex(aConfig)
	includeIndex := s.buildIncludeIndex(aConfig)

	for _, column := range columns {
		if s.shouldSkipColumn(exceptIndex, includeIndex, column) {
			continue
		}
		meta, err := s.buildFieldMeta(column, pkIndex, fkIndex)
		if err != nil {
			return nil, err
		}

		aType, err := view.GetOrParseType(dConfig.Config.LookupType, column.Type)
		if err != nil {
			return nil, err
		}

		tagContent := "name=" + column.Name
		if meta.autoincrement {
			tagContent += ",autoincrement"
		} else if meta.generator != "" {
			tagContent += ",generator=" + meta.generator
		}
		var jsonTag string
		if !meta.required {
			jsonTag = ` json:",omitempty"`
		}

		sqlxTagContent := tagContent

		if meta.primaryKey {
			sqlxTagContent += ",primaryKey"
		} else if key := meta.fkKey; key != nil {
			sqlxTagContent += ",refTable=" + key.ReferenceTable
			sqlxTagContent += ",refColumn=" + key.ReferenceColumn
		} else if column.IsUnique() {
			sqlxTagContent += ",unique,table=" + table
		}

		if (!column.IsNullable()) && (column.IsAutoincrement == nil || (column.IsAutoincrement != nil && !*column.IsAutoincrement) || column.Default == nil) {
			sqlxTagContent += ",required"
		}
		validationTag := "omitempty"
		if strings.Contains(strings.ToLower(column.Name), "email") {
			validationTag += ",email"
		}
		if strings.Contains(strings.ToLower(column.Name), "phone") {
			validationTag += ",phone"
		}
		if validationTag == "omitempty" {
			validationTag = ""
		} else {
			validationTag = fmt.Sprintf(` validate:"%v"`, validationTag)
		}
		aTag := fmt.Sprintf(`sqlx:"%v"%v%v`, sqlxTagContent, jsonTag, validationTag)

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

	actualPath := structPath
	if parentTable != "" {
		actualPath += paramName + "/"
	}

	insertRelations, err := s.buildInsertRelations(aConfig, db, actualPath, actualHolder)
	if err != nil {
		return nil, err
	}

	for _, relation := range insertRelations {
		relField := &view.Field{
			Name:        relation.paramName,
			Fields:      relation.typeDef.Fields,
			Tag:         fmt.Sprintf(`typeName:"%v" sqlx:"-"`, relation.paramName),
			Cardinality: relation.config.outputConfig.Cardinality,
			Ptr:         true,
		}
		definition.Fields = append(definition.Fields, relField)
	}

	hasFieldName := "Has"
	hasField := &view.Field{
		Name: hasFieldName,
		Tag:  fmt.Sprintf(`%v:"true" typeName:"%v" json:"-" sqlx:"presence=true"`, json2.IndexKey, definition.Name+"Has"),
		Ptr:  true,
	}

	for _, field := range actualFields {
		hasField.Fields = append(hasField.Fields, &view.Field{Name: field.Name, Schema: &view.Schema{DataType: "bool"}})
	}

	definition.Fields = append(definition.Fields, hasField)

	if !aConfig.outputConfig.IsBasic() {
		holderName = aConfig.outputConfig.GetField()
		definition.Name = holderName
		definition.Fields = []*view.Field{
			{
				Name:        aConfig.outputConfig.Field,
				Fields:      definition.Fields,
				Cardinality: definition.Cardinality,
				Tag:         fmt.Sprintf(`typeName:"%v"`, paramName),
				Ptr:         true,
			},
		}
		definition.Cardinality = ""
	}

	sort.Sort(viewFields(actualFields))

	SQL, err := s.buildInputMetadataSQL(actualFields, typesMeta, table, paramName, actualPath, actualHolder)
	if err != nil {
		return nil, err
	}

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
		config:       aConfig,
		sql:          SQL,
		sqlName:      paramName + "DBRecords",
		isPtr:        true,
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

func filterKeys(keys []sink.Key, tableName string) []sink.Key {
	var tableKeys []sink.Key
	for i, aKey := range keys {
		if aKey.Table == tableName {
			tableKeys = append(tableKeys, keys[i])
		}
	}
	return tableKeys
}

func (s *Builder) buildInsertRelations(config *viewConfig, db *sql.DB, path string, holder string) ([]*inputMetadata, error) {
	var relations []*inputMetadata
	for _, relation := range config.relations {
		relationConfig, err := s.detectInputType(context.TODO(), db, relation.expandedTable.Name, relation, config.expandedTable.Name, path, holder)
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

func (b *stmtBuilder) appendHintsWithRelations() error {
	return b.iterateOverHints(b.typeDef, func(metadata *inputMetadata) error {
		return b.appendHints(metadata)
	})
}

func (b *stmtBuilder) appendSQLWithRelations() error {
	return b.iterateOverHints(b.typeDef, func(metadata *inputMetadata) error {
		return b.appendSQLHint(metadata)
	})
}

func (b *stmtBuilder) appendSQLHint(metadata *inputMetadata) error {
	sqlHint, err := b.paramSQLHint(metadata)
	if err != nil {
		return err
	}
	b.writeString(fmt.Sprintf("\n#set($_ = $%v ", metadata.sqlName))
	b.writeString(b.stringWithIndent(sqlHint))
	b.writeString("\n)")
	return nil
}

func (b *stmtBuilder) appendHints(typeDef *inputMetadata) error {
	hint, err := b.paramHint(typeDef)
	if err != nil {
		return err
	}

	b.writeString(fmt.Sprintf("\n#set($_ = $%v%v)", typeDef.paramName, hint))
	return nil
}

func (b *stmtBuilder) iterateOverHints(metadata *inputMetadata, iterator func(*inputMetadata) error) error {
	if err := iterator(metadata); err != nil {
		return err
	}

	for _, relation := range metadata.relations {
		if err := iterator(relation); err != nil {
			return err
		}
	}

	return nil
}

func (b *stmtBuilder) stringWithIndent(value string) string {
	return strings.ReplaceAll(value, "\n", "\n"+defaultIndent)
}

func (b *stmtBuilder) withIndent() *stmtBuilder {
	aCopy := *b
	aCopy.indent += defaultIndent
	return &aCopy
}

func (b *stmtBuilder) paramSQLHint(def *inputMetadata) (string, error) {
	paramOption := &option.ParameterConfig{
		Required: boolPtr(false),
	}

	marshal, err := json.Marshal(paramOption)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("/* %v %v */", string(marshal), def.sql), nil
}

func (s *Builder) buildInputMetadataSQL(fields []*view.Field, meta *typeMeta, table string, paramName string, path string, holder string) (string, error) {
	vConfig := &option.ViewConfig{
		Selector: &view.Config{},
	}

	marshal, err := json.Marshal(vConfig)
	if err != nil {
		return "", err
	}

	sb := &strings.Builder{}
	sqlSb := &strings.Builder{}
	sqlSb.WriteString("SELECT * FROM ")
	sqlSb.WriteString(table)
	sqlSb.WriteString(fmt.Sprintf("/* %v */", string(marshal)))

	setSb := &strings.Builder{}

	var i int
	for _, field := range fields {
		aFieldMeta, ok := meta.metaByColName(field.Column)
		if !ok || !aFieldMeta.primaryKey {
			continue
		}

		if i == 0 {
			sqlSb.WriteString("\nWHERE ")
		} else {
			sqlSb.WriteString("\nAND ")
		}
		i++

		recName := s.appendSelectField(aFieldMeta, setSb, holder, path)
		sqlSb.WriteString(fmt.Sprintf(" #if($%v.Length() > 0 ) ", recName))
		sqlSb.WriteString(aFieldMeta.columnName)
		sqlSb.WriteString(" IN ( $")
		sqlSb.WriteString(recName)
		sqlSb.WriteString(" ) ")
		sqlSb.WriteString("#else 1 = 0 #end")
	}

	sb.WriteString(setSb.String())
	sb.WriteString("\n")
	sb.WriteString(sqlSb.String())
	return sb.String(), nil
}

func (s *Builder) appendSelectField(aFieldMeta *fieldMeta, sb *strings.Builder, paramName string, path string) string {
	recName := paramName + aFieldMeta.fieldName
	sb.WriteString(fmt.Sprintf(
		"\n#set($%v = $%v.QueryFirst(\"SELECT ARRAY_AGG(%v) AS Values FROM  `%v`\"))",
		recName, paramName, aFieldMeta.fieldName, path,
	))
	return recName + "." + "Values"
}

func (b *patchStmtBuilder) generateIndexes() ([]*indexChecker, error) {
	var checkers []*indexChecker
	err := b.iterateOverHints(b.typeDef, func(def *inputMetadata) error {
		for _, field := range def.actualFields {
			aMeta, ok := def.meta.metaByColName(field.Column)
			if !ok || !aMeta.primaryKey {
				continue
			}

			indexName, aFieldName := b.appendIndex(def, aMeta)

			checkers = append(checkers, &indexChecker{
				indexName: indexName,
				field:     aFieldName,
				paramName: def.paramName,
			})
		}

		return nil
	})

	return checkers, err
}

func (s *Builder) prepareStringBuilder(typeDef *inputMetadata, config *viewConfig, routeOption *option.RouteConfig) (*strings.Builder, error) {
	sb := &strings.Builder{}
	typeName := typeDef.typeDef.Name

	paramType, err := s.buildRequestBodyPostParam(config, typeDef)
	if err != nil {
		return nil, err
	}

	if err = s.uploadGoType(typeName, paramType, routeOption, config); err != nil {
		return nil, err
	}

	if err = s.appendMetadata(typeDef.paramName, routeOption, typeName, typeDef, sb); err != nil {
		return nil, err
	}

	return sb, nil
}

func (s *Builder) appendMetadata(paramName string, routeOption *option.RouteConfig, typeName string, typeDef *inputMetadata, sb *strings.Builder) error {
	routeOption.ResponseBody = &option.ResponseBodyConfig{
		From: paramName,
	}

	marshal, err := json.Marshal(routeOption)
	if err != nil {
		return err
	}

	if routeJSON := string(marshal); routeJSON != "{}" {
		sb.WriteString(fmt.Sprintf("/* %v */\n", routeJSON))
	}

	requiredTypes := []string{typeDef.paramName}
	if typeDef.bodyHolder != "" {
		requiredTypes = append(requiredTypes, typeDef.bodyHolder)
	}

	if len(requiredTypes) > 0 {
		sb.WriteString("\nimport (")
		for _, requiredType := range requiredTypes {
			URL := s.goURL("")
			if ext := path.Ext(URL); ext != "" {
				URL = path.Dir(URL)
			}

			if s.options.RelativePath != "" && strings.HasPrefix(URL, s.options.RelativePath) {
				URL = strings.Replace(URL, s.options.RelativePath, "", 1)
				if len(URL) > 0 && URL[0] == '/' {
					URL = URL[1:]
				}
			}

			sb.WriteString(fmt.Sprintf("\n	\"%v.%v\"", URL, requiredType))
		}
		sb.WriteString("\n)\n\n")
	}

	return nil
}

func (s *Builder) extractRouteSettings(sourceSQL []byte) (string, string) {
	hint := sanitize.ExtractHint(string(sourceSQL))
	SQL := strings.Replace(string(sourceSQL), hint, "", 1)
	return hint, SQL
}

func (s *Builder) uploadGoType(name string, rType reflect.Type, routeOption *option.RouteConfig, config *viewConfig) error {
	goURL := s.goURL(name)
	modulePath, isXDatly := s.IsPluginBundle(goURL)

	content, err := s.generateGoFileContent(name, rType, routeOption, config, modulePath, routeOption.Package)
	if err != nil {
		return err
	}

	if _, err = s.upload(goURL, string(content)); err != nil {
		return err
	}

	if isXDatly {
		if err = s.registerXDatlyGoFile(modulePath, goURL); err != nil {
			return err
		}
	}

	sampleValue := getStruct(rType)
	sample := sampleValue.Interface()
	if data, err := json.Marshal(sample); err == nil {
		_, _ = s.uploadRuleFile(s.options.DSQLOutput, name+"Post", string(data), ".json", true)
	}

	return nil
}

func (s *Builder) registerXDatlyGoFile(modulePath string, outputURL string) error {
	var imports []string
	types, err := xreflect.ParseTypes(path.Join(modulePath, "imports"))
	if err == nil {
		imports = types.Imports("*" + importsFile)
	}

	location := s.relativeOf(modulePath, outputURL)
	imports = append(imports, location)
	result := &bytes.Buffer{}
	result.WriteString(`// Code generated by DATLY. Append sideefect imports here.

		package imports
		
		import (`)

	imported := map[string]bool{}
	for _, packageName := range imports {
		if imported[packageName] {
			continue
		}

		result.WriteString(fmt.Sprintf(`
		_ "%v"
`, packageName))

		imported[packageName] = true
	}

	result.WriteString("\n)")

	source, err := goFormat.Source(result.Bytes())
	if err != nil {
		return err
	}

	registryURL := path.Join(modulePath, "imports", importsFile)
	return s.fs.Upload(context.Background(), registryURL, file.DefaultFileOsMode, bytes.NewReader(source))
}

func (s *Builder) relativeOf(modulePath string, outputURL string) string {
	var segments []string
	outputURL = path.Dir(outputURL)
	for len(outputURL) > 1 && outputURL != modulePath {
		fmt.Printf("ModulePath: %v | OutputPath: %v\n", modulePath, outputURL)

		name := path.Base(outputURL)
		segments = append([]string{name}, segments...)
		outputURL = path.Dir(outputURL)
	}

	URL := url.Join(customTypesModule, segments...)
	fmt.Printf("Generated URL: %v\n", URL)
	return URL
}

func (s *Builder) generateGoFileContent(name string, rType reflect.Type, routeOption *option.RouteConfig, aConfig *viewConfig, xDatlyModURL string, packageName string) ([]byte, error) {
	if packageName == "" {
		base := path.Base(aConfig.fileName)
		ext := path.Ext(base)
		packageName = strings.Replace(base, ext, "", 1)
	}

	sbBefore := &bytes.Buffer{}
	sbBefore.WriteString(fmt.Sprintf("var PackageName = \"%v\"\n", packageName))

	var imports xreflect.Imports
	if xDatlyModURL == "" {
		sbBefore.WriteString(fmt.Sprintf(`
var %v = map[string]reflect.Type{
		"%v": reflect.TypeOf(%v{}),
}
`, dConfig.TypesName, name, name))

		imports = append(imports, "reflect")
		packageName = "main"
	} else {
		imports = append(imports,
			coreTypesModule,
			generatedCustomTypesModule,
			"reflect",
		)

		sbBefore.WriteString(fmt.Sprintf(`
func init() {
	core.RegisterType(PackageName, "%v", reflect.TypeOf(%v{}), generated.GeneratedTime)
}
`, name, name))
	}

	sb := &bytes.Buffer{}
	generatedStruct := xreflect.GenerateStruct(name, rType, imports, xreflect.AppendBeforeType(sbBefore.String()), xreflect.PackageName(packageName))
	sb.WriteString(generatedStruct)
	sb.WriteString("\n")

	source, err := goFormat.Source(sb.Bytes())
	if err != nil {
		return nil, err
	}

	return source, nil
}

func getStruct(rType reflect.Type) reflect.Value {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	sampleValue := reflect.New(rType)
	for i := 0; i < rType.NumField(); i++ {
		fieldType := rType.Field(i).Type
		isPtr := false
		if isPtr = fieldType.Kind() == reflect.Ptr; isPtr {
			fieldType = fieldType.Elem()
		}
		fieldValue := sampleValue.Elem().Field(i)

		if fieldType.Kind() == reflect.String {
			str := " "
			if isPtr {
				fieldValue.Set(reflect.ValueOf(&str))
			} else {
				fieldValue.Set(reflect.ValueOf(str))
			}
		}
		if fieldType.Kind() == reflect.Slice {
			aSlice := reflect.MakeSlice(fieldType, 1, 1)

			itemType := fieldType.Elem()
			isItemPtr := itemType.Kind() == reflect.Ptr
			if isItemPtr {
				itemType = itemType.Elem()
			}
			if itemType.Kind() == reflect.Struct {
				itemValue := getStruct(itemType)
				if isItemPtr {
					aSlice.Index(0).Set(itemValue)
				} else {
					aSlice.Index(0).Set(itemValue.Elem())
				}
			}
			fieldValue.Set(aSlice)
		}
		if fieldType.Kind() == reflect.Struct && fieldType != xreflect.TimeType {
			newFieldValue := reflect.New(fieldType)
			elemField := fieldValue

			if fieldValue.IsZero() {
				initializedValue := reflect.New(fieldValue.Type())
				elemField = initializedValue.Elem()
				fieldValue.Set(elemField)
			}
			elemField.Set(newFieldValue)
		}
	}
	return sampleValue
}

func (b *stmtBuilder) appendOptionalIfNeeded(accessor *paramAccessor, stack *Stack) bool {
	isOptional := b.typeDef.isPtr
	if isOptional {
		b.writeString(fmt.Sprintf("\n#if($%v)", accessor.unsafeRecord))
		stack.Push(b)
	}
	return isOptional
}
