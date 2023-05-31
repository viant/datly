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
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
	qexpr "github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
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

	idParam struct {
		name  string
		query string
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

func (b *stmtBuilder) appendIndex(def *inputMetadata, aMeta *fieldMeta, inputPath []*inputMetadata, previous bool) (string, string) {
	aFieldName := aMeta.fieldName
	indexName := fmt.Sprintf("%vBy%v", def.indexNamePrefix, strings.Title(aFieldName))
	src := def.prevNamePrefix
	if previous && len(inputPath) > 0 {
		actualPath := strings.Trim(def.path, "/")
		if actualPath == "" {
			actualPath = "/"
		} else {
			actualPath = "/" + actualPath + "/"
		}

		src = fmt.Sprintf("%v.Query(\"SELECT * FROM `%v`\")", inputPath[0].prevNamePrefix, actualPath)
	}

	b.sb.WriteString("\n")
	b.writeString(fmt.Sprintf("#set($%v = $%v.IndexBy(\"%v\"))", indexName, src, aFieldName))
	return indexName, aFieldName
}

func (s *Builder) buildInputMetadata(ctx context.Context, builder *routeBuilder, sourceSQL []byte, httpMethod string) (*option.RouteConfig, *ViewConfig, *inputMetadata, error) {
	hint, SQL := s.extractRouteSettings(sourceSQL)

	routeOption := &option.RouteConfig{Method: httpMethod}
	if err := tryUnmarshalHint(hint, routeOption); err != nil {
		return nil, nil, nil, err
	}

	paramIndex := NewParametersIndex(routeOption, map[string]*sanitize.ParameterHint{})

	configurer, err := NewConfigProviderReader("", SQL, routeOption, router.ReaderServiceType, paramIndex, &s.options.Prepare, &s.options.Connector, builder)
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
	joinSQL := ""
	if join := aConfig.queryJoin; join != nil {
		joinSQL = sqlparser.Stringify(aConfig.queryJoin.With)
	}
	paramType, err := s.detectInputType(ctx, db, aConfig.expandedTable.Name, joinSQL, aConfig, "", "/", "")
	if err != nil {
		return nil, nil, nil, err
	}
	return routeOption, aConfig, paramType, nil
}

func (s *Builder) detectInputType(ctx context.Context, db *sql.DB, tableName, SQL string, config *ViewConfig, parentTable, path, actualHolder string) (*inputMetadata, error) {

	var columns []sink.Column
	var err error
	if SQL != "" {
		if columns, err = s.detectSinkColumn(ctx, db, SQL); err != nil {
			return nil, err
		}
	}
	if len(columns) == 0 {
		if columns, err = s.readSinkColumns(ctx, db, tableName); err != nil {
			return nil, err
		}
	}

	//fmt.Printf("column: %v %+v\n--------\n\n\n"+
	//	""+
	//	""+
	//	"", tableName, columns)
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
	if tableName == "" {
		return nil, nil
	}
	meta := metadata.New()
	var keys []sink.Key
	if err := meta.Info(ctx, db, info.KindForeignKeys, &keys); err != nil {
		return nil, err
	}

	return filterKeys(keys, tableName), nil
}

func (s *Builder) readPrimaryKeys(ctx context.Context, db *sql.DB, tableName string) ([]sink.Key, error) {
	if tableName == "" {
		return nil, nil
	}
	meta := metadata.New()
	var keys []sink.Key
	if err := meta.Info(ctx, db, info.KindPrimaryKeys, &keys); err != nil {
		return nil, err
	}

	return filterKeys(keys, tableName), nil
}

func (s *Builder) buildPostInputParameterType(columns []sink.Column, foreignKeys, primaryKeys []sink.Key, aConfig *ViewConfig, db *sql.DB, table, parentTable, structPath, actualHolder string) (*inputMetadata, error) {
	if aConfig.outputConfig.Cardinality == "" {
		aConfig.outputConfig.Cardinality = view.Many
	}

	fkIndex := s.indexKeys(foreignKeys)
	pkIndex := s.indexKeys(primaryKeys)

	typesMeta := &typeMeta{fieldIndex: map[string]int{}, columnIndex: map[string]int{}}
	name := aConfig.expandedTable.HolderName

	detectCase, err := format.NewCase(formatter.DetectCase(name))
	if err != nil {
		return nil, err
	}

	if detectCase != format.CaseUpperCamel {
		name = detectCase.Format(name, format.CaseUpperCamel)
	}

	cardinality := view.Many
	if s.isToOne(aConfig.queryJoin) {
		cardinality = view.One
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

	var fieldByName = map[string]bool{}
	for _, column := range columns {
		if s.shouldSkipColumn(exceptIndex, includeIndex, column) {
			continue
		}
		meta, err := s.buildFieldMeta(column, pkIndex, fkIndex)
		if err != nil {
			return nil, err
		}

		aType, err := types.GetOrParseType(dConfig.Config.LookupType, column.Type)
		if err != nil {
			return nil, err
		}

		tagContent := "name=" + column.Name
		if meta.autoincrement {
			tagContent += ",autoincrement"
		}

		// supper for know as there are some problems
		//} else if meta.generator != "" {
		//	tagContent += ",generator=" + meta.generator
		//}

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

		if fieldByName[meta.fieldName] {
			panic(fmt.Sprintf("filed already added %v, %v -- %v", meta.fieldName, table, parentTable))
			continue
		}
		fieldByName[meta.fieldName] = true
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

	insertRelations, err := s.buildExecRelations(aConfig, db, actualPath, actualHolder)
	if err != nil {
		return nil, err
	}
	for _, relation := range insertRelations {

		datlyTagSpec := s.buildDatlyTagSpec(relation)
		if s.isToOne(relation.config.queryJoin) {
			relation.config.outputConfig.Cardinality = view.One
		}
		//relation.config.outputConfig.Cardinality
		relField := &view.Field{
			Name:        relation.paramName,
			Fields:      relation.typeDef.Fields,
			Tag:         fmt.Sprintf(`typeName:"%v" sqlx:"-"`+datlyTagSpec, relation.paramName),
			Cardinality: relation.config.outputConfig.Cardinality,
			Ptr:         true,
		}
		definition.Fields = append(definition.Fields, relField)
	}

	hasFieldName := "Has"
	hasField := &view.Field{
		Name: hasFieldName,
		Tag:  fmt.Sprintf(`%v:"true" typeName:"%v" json:"-" diff:"presence=true" sqlx:"presence=true" validate:"presence=true"`, json2.IndexKey, definition.Name+"Has"),
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

	SQL, idParams, err := s.buildInputMetadataSQL(actualFields, typesMeta, table, aConfig.expandedTable.SQL, paramName, actualPath, actualHolder)
	if err != nil {
		return nil, err
	}

	return &inputMetadata{
		typeDef:         definition,
		meta:            typesMeta,
		actualFields:    actualFields,
		paramName:       paramName,
		bodyHolder:      holderName,
		relations:       insertRelations,
		fkIndex:         fkIndex,
		pkIndex:         pkIndex,
		table:           table,
		config:          aConfig,
		sql:             SQL,
		prevNamePrefix:  "cur" + strings.Title(paramName),
		indexNamePrefix: strings.ToLower(paramName[0:1]) + paramName[1:],
		isPtr:           true,
		path:            strings.TrimRight(actualPath, "/"),
		idParams:        idParams,
	}, nil
}

func (s *Builder) isToOne(join *query.Join) bool {
	if join == nil {
		return false
	}
	return strings.Contains(sqlparser.Stringify(join.On), "1 = 1")
}

func (c *ViewConfig) IsToMany() bool {
	if c.parent == nil {
		return c.outputConfig.IsMany()
	}
	if c.parent.IsToMany() {
		return true
	}
	return false
}

func (s *Builder) buildDatlyTagSpec(relation *inputMetadata) string {
	datlyTagSpec := ""
	if join := relation.config.queryJoin; join != nil {
		relColumn, refColumn := extractRelationColumns(join)
		source := ""
		if refTable := relation.table; refTable != "" {
			source = fmt.Sprintf("refTable=%v", refTable)
		}
		datlyTagSpec = " " + fmt.Sprintf(view.DatlyTag+`:"relName=%v,relColumn=%v,refColumn=%v,%v"`, join.Alias, relColumn, refColumn, source)
		if rawSQL := strings.Trim(sqlparser.Stringify(join.With), " )("); rawSQL != "" {
			datlyTagSpec += ` sql:"` + strings.ReplaceAll(rawSQL, "\n", " ") + `"`
		}
	}
	return datlyTagSpec
}

func extractRelationColumns(join *query.Join) (string, string) {
	relColumn := ""
	refColumn := ""
	sqlparser.Traverse(join.On, func(n node.Node) bool {
		switch actual := n.(type) {
		case *qexpr.Binary:
			if xSel, ok := actual.X.(*qexpr.Selector); ok {
				if xSel.Name == join.Alias {
					refColumn = sqlparser.Stringify(xSel.X)
				} else if relColumn == "" {
					relColumn = sqlparser.Stringify(xSel.X)
				}
			}
			if ySel, ok := actual.Y.(*qexpr.Selector); ok {
				if ySel.Name == join.Alias {
					refColumn = sqlparser.Stringify(ySel.X)
				} else if relColumn == "" {
					relColumn = sqlparser.Stringify(ySel.X)
				}
			}
			return true
		}
		return true
	})
	return relColumn, refColumn
}

func extractRelationAliases(join *query.Join) (string, string) {
	relAlias := ""
	refAlias := ""
	sqlparser.Traverse(join.On, func(n node.Node) bool {
		switch actual := n.(type) {
		case *qexpr.Binary:
			if xSel, ok := actual.X.(*qexpr.Selector); ok {

				if xSel.Name == join.Alias {

					refAlias = xSel.Name
				} else if relAlias == "" {
					relAlias = xSel.Name
				}
			}
			if ySel, ok := actual.Y.(*qexpr.Selector); ok {
				if ySel.Name == join.Alias {
					refAlias = ySel.Name
				} else if relAlias == "" {
					relAlias = ySel.Name
				}
			}
			return true
		}
		return true
	})
	return relAlias, refAlias
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

func (s *Builder) buildExecRelations(config *ViewConfig, db *sql.DB, path string, holder string) ([]*inputMetadata, error) {
	var relations []*inputMetadata
	for _, relation := range config.relations {
		SQL := sqlparser.Stringify(relation.queryJoin.With)
		relation.isVirtual = isVirtualQuery(SQL)
		tableName := relation.expandedTable.Name
		relationConfig, err := s.detectInputType(context.TODO(), db, tableName, SQL, relation, config.expandedTable.Name, path, holder)
		if err != nil {
			return nil, err
		}
		relations = append(relations, relationConfig)
	}

	return relations, nil
}

func isVirtualQuery(SQL string) bool {
	SQL = strings.Trim(strings.TrimSpace(SQL), "()")
	query, _ := sqlparser.ParseQuery(SQL)
	if query == nil {
		return true
	}
	from := sqlparser.Stringify(query.From.X)
	return strings.Contains(from, "(")
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

func (s *Builder) buildExceptIndex(config *ViewConfig) map[string]bool {
	exceptIndex := map[string]bool{}
	for _, column := range config.expandedTable.Columns {
		for _, except := range column.Except {
			exceptIndex[strings.ToLower(except)] = true
		}
	}

	return exceptIndex
}

func (s *Builder) buildIncludeIndex(config *ViewConfig) map[string]bool {
	includeIndex := map[string]bool{}
	for _, column := range config.expandedTable.Inner {
		if column.Name == "*" {
			return includeIndex
		}
	}

	for _, column := range config.expandedTable.Inner {
		if column.Alias != "" {
			includeIndex[strings.ToLower(column.Alias)] = true
		}
		includeIndex[strings.ToLower(column.Name)] = true

	}
	return includeIndex
}

func (s *Builder) buildFieldMeta(column sink.Column, pkIndex map[string]sink.Key, fkIndex map[string]sink.Key) (*fieldMeta, error) {
	columnCase, err := format.NewCase(formatter.DetectCase(column.Name))
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
	return b.appendHints(b.typeDef)
}

func (b *stmtBuilder) appendSQLWithRelations(loadPrevious bool, preSQL string) error {
	if loadPrevious {
		var qualifiers []*option.Qualifier
		var viewHint string
		for _, field := range b.typeDef.actualFields {
			if strings.Contains(strings.ToLower(field.Tag), "primarykey") {
				qualifiers = append(qualifiers, &option.Qualifier{
					Column: field.Column,
					Value:  b.paramName + "." + field.Name,
				})
			}
		}

		if len(qualifiers) > 0 {
			vConfig := option.ParameterConfig{
				Qualifiers: qualifiers,
			}

			marshal, _ := json.Marshal(vConfig)
			viewHint = string(marshal)
		}

		b.appendParamHint(b.typeDef.prevNamePrefix, fmt.Sprintf("/* %v %v */", viewHint, preSQL), "", "", "")
		return nil
	}

	return b.IterateOverHints(b.typeDef, func(metadata *inputMetadata, _ []*inputMetadata) error {
		return b.appendSQLHint(b.typeDef, metadata)
	})
}

func (b *stmtBuilder) appendSQLHint(main, metadata *inputMetadata) error {
	for _, param := range metadata.idParams {
		b.appendParamHint(param.name, fmt.Sprintf("/* \n %v \n */", param.query), "", string(view.KindParam), main.paramName)
	}

	sqlHint, err := b.paramSQLHint(metadata)
	if err != nil {
		return err
	}

	multi := ""
	if metadata.typeDef != nil && (metadata.config.IsToMany() || metadata.typeDef.Cardinality == view.Many) {
		multi = "[]"
	}
	resultType := multi + "*" + metadata.paramName
	in := "data_view"
	target := metadata.prevNamePrefix
	b.appendParamHint(metadata.prevNamePrefix, sqlHint, resultType, in, target)

	return nil
}

func (b *stmtBuilder) appendParamHint(paramName string, hint string, resultType string, in string, target string) {
	artificialParam := fmt.Sprintf("\n#set($_ = $%v", paramName)
	if resultType == "" && in != "" {
		resultType = "?"
	}

	if resultType != "" {
		artificialParam += fmt.Sprintf("<%v>", resultType)
	}

	if in != "" {
		artificialParam += fmt.Sprintf("(%v/%v)", in, target)
	}

	artificialParam += " "
	b.writeString(artificialParam)
	b.writeString(b.stringWithIndent(hint))
	b.writeString("\n)")
}

func (b *stmtBuilder) appendHints(typeDef *inputMetadata) error {
	hint, err := b.paramHint(typeDef)
	if err != nil {
		return err
	}

	paramDeclaration := fmt.Sprintf("\n#set($_ = $%v%v)", typeDef.paramName, hint)
	b.writeString(paramDeclaration)
	return nil
}

func (b *stmtBuilder) IterateOverHints(metadata *inputMetadata, iterator func(*inputMetadata, []*inputMetadata) error) error {
	return b.iterateOverHints(metadata, iterator)
}

func (b *stmtBuilder) iterateOverHints(metadata *inputMetadata, iterator func(curr *inputMetadata, metadataPath []*inputMetadata) error, path ...*inputMetadata) error {
	if err := iterator(metadata, path); err != nil {
		return err
	}

	aPath := append([]*inputMetadata{}, path...)
	aPath = append(aPath, b.typeDef)

	for _, relation := range metadata.relations {
		if err := b.iterateOverHints(relation, iterator, aPath...); err != nil {
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
	//paramOption := &option.ParameterConfig{
	//	Required: boolPtr(false),
	//}

	//marshal, err := json.Marshal(paramOption)
	//if err != nil {
	//	return "", err
	//}
	marshal := "?"
	return fmt.Sprintf("/* %v %v \n*/", string(marshal), def.sql), nil
}

func (s *Builder) buildInputMetadataSQL(fields []*view.Field, meta *typeMeta, table, SQL string, paramName string, aPath string, holder string) (string, []*idParam, error) {
	//vConfig := &option.ViewConfig{
	//	Selector: &view.Config{},
	//}

	var idParams []*idParam
	//marshal, err := json.Marshal(vConfig)
	//	if err != nil {
	//		return "", nil, err
	//	}
	sb := &strings.Builder{}
	sqlSb := &strings.Builder{}

	if SQL != "" {
		sqlSb.WriteString(SQL)

	} else {
		sqlSb.WriteString("SELECT * FROM ")
		sqlSb.WriteString(table)
	}
	//sqlSb.WriteString(fmt.Sprintf("/* %v */", string(marshal)))

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

		recName := s.getStructQLName(aFieldMeta, paramName)
		sqlSb.WriteString(fmt.Sprintf(`$criteria.In("%v", $%v.Values)`, aFieldMeta.columnName, recName))

		idParams = append(idParams, &idParam{
			name:  recName,
			query: s.getStructSQLParam(aFieldMeta, aPath),
		})
	}

	sb.WriteString(setSb.String())
	sb.WriteString("\n")
	sb.WriteString(sqlSb.String())
	return sb.String(), idParams, nil
}

func (s *Builder) getStructSQLParam(aFieldMeta *fieldMeta, path string) string {
	qlQuery := fmt.Sprintf("SELECT ARRAY_AGG(%v) AS Values FROM  `%v` LIMIT 1", aFieldMeta.fieldName, path)
	return qlQuery
}

func (s *Builder) getStructQLName(aFieldMeta *fieldMeta, paramName string) string {
	return paramName + aFieldMeta.fieldName
}

func (b *stmtBuilder) generateIndexes(loadPrevious bool, ensureIndexes bool) ([]*indexChecker, error) {
	var checkers []*indexChecker
	err := b.IterateOverHints(b.typeDef, func(def *inputMetadata, inputPath []*inputMetadata) error {
		if !def.config.unexpandedTable.ViewConfig.FetchRecords && !ensureIndexes {
			return nil
		}

		for _, field := range def.actualFields {
			aMeta, ok := def.meta.metaByColName(field.Column)
			if !ok || !aMeta.primaryKey {
				continue
			}

			indexName, aFieldName := b.appendIndex(def, aMeta, inputPath, loadPrevious)

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

func (s *Builder) prepareStringBuilder(routeBuilder *routeBuilder, typeDef *inputMetadata, config *ViewConfig, routeOption *option.RouteConfig) (*strings.Builder, error) {
	sb := &strings.Builder{}
	typeName := typeDef.typeDef.Name

	paramType, err := s.buildRequestBodyPostParam(typeDef)
	if err != nil {
		return nil, err
	}

	if err = s.uploadGoType(routeBuilder, typeName, paramType, routeOption, config); err != nil {
		return nil, err
	}

	if err = s.appendMetadata(routeBuilder, typeDef.paramName, routeOption, typeName, typeDef, sb); err != nil {
		return nil, err
	}

	return sb, nil
}

func (s *Builder) appendMetadata(builder *routeBuilder, paramName string, routeOption *option.RouteConfig, typeName string, typeDef *inputMetadata, sb *strings.Builder) error {
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

	for _, r := range typeDef.relations {
		if r.typeDef.Name != "" {
			requiredTypes = append(requiredTypes, r.typeDef.Name)
		}
	}

	if len(requiredTypes) > 0 {
		sb.WriteString("\nimport (\n")

		for _, requiredType := range requiredTypes {
			importPath := s.adjustImportPath(builder)
			sb.WriteString(fmt.Sprintf("\n	\"%v.%v\"", strings.TrimRight(importPath, "/"), requiredType))
		}
		sb.WriteString("\n)\n\n")
	}

	return nil
}

func (s *Builder) adjustImportPath(builder *routeBuilder) string {
	URL := builder.session.GoFileURL("")
	if ext := path.Ext(URL); ext != "" {
		URL = path.Dir(URL)
	}

	if s.options.RelativePath != "" && strings.HasPrefix(URL, s.options.RelativePath) {
		URL = strings.Replace(URL, s.options.RelativePath, "", 1)
		if len(URL) > 0 && URL[0] == '/' {
			URL = strings.ToLower(URL[1:])
		}
	}
	return URL
}

func (s *Builder) extractRouteSettings(sourceSQL []byte) (string, string) {
	hint := sanitize.ExtractHint(string(sourceSQL))
	SQL := strings.Replace(string(sourceSQL), hint, "", 1)
	return hint, SQL
}

func (s *Builder) uploadGoType(builder *routeBuilder, name string, rType reflect.Type, routeOption *option.RouteConfig, config *ViewConfig) error {
	goURL := builder.session.GoFileURL(s.fileNames.unique(name)) + ".go"

	modBundle, isXDatly := s.isPluginBundle(goURL)
	modulePackage := s.options.Prepare.GoModulePkg
	content, err := s.generateGoFileContent(name, rType, routeOption, config, modBundle, modulePackage)
	if err != nil {
		return err
	}

	if _, err = s.upload(builder, goURL, string(content)); err != nil {
		return err
	}

	if isXDatly {
		if err = s.registerXDatlyGoFile(modBundle, goURL); err != nil {
			return err
		}
	}

	sampleValue := getStruct(rType)
	sample := sampleValue.Interface()
	if data, err := json.Marshal(sample); err == nil {
		_, _ = s.upload(builder, builder.session.SampleFileURL(name+"Post")+".json", string(data))
	}

	return nil
}

func (s *Builder) registerXDatlyGoFile(moduleBundle *bundleMetadata, outputURL string) error {
	var imports []string

	goModulePath := moduleBundle.moduleName
	if ok, _ := s.fs.Exists(context.Background(), goModulePath); !ok {
		goModulePath = moduleBundle.url
	}

	types, err := xreflect.ParseTypes(path.Join(goModulePath, importsDirectory), xreflect.TypeLookupFn(dConfig.Config.LookupType))
	if err == nil {
		imports = types.Imports("*" + importsFile)
	}

	location := s.relativeOf(moduleBundle.url, outputURL, moduleBundle.moduleName)
	imports = append(imports, location)
	result := &bytes.Buffer{}
	result.WriteString(`// Code generated by DATLY. Append sideefect imports here.

		package dependency
		
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
		return fmt.Errorf("failed to generate dep go code: %w, %s", err, source)
	}

	registryURL := path.Join(moduleBundle.url, importsDirectory, importsFile)
	shouldInitXDatlyMod := moduleBundle != nil && moduleBundle.shouldInitXDatlyModule()
	if shouldInitXDatlyMod {
		_ = s.fs.Create(context.Background(), path.Dir(registryURL), file.DefaultDirOsMode, true)
	}

	if err = s.fs.Upload(context.Background(), registryURL, file.DefaultFileOsMode, bytes.NewReader(source)); err != nil {
		return err
	}

	if shouldInitXDatlyMod {
		if err = s.ensureSideefectsImports(moduleBundle); err != nil {
			return err
		}

		if err = s.ensureChecksum(moduleBundle); err != nil {
			return err
		}

		if err = s.ensurePlugins(moduleBundle); err != nil {
			return err
		}
	}

	return nil
}

func (s *Builder) relativeOf(modulePath string, outputURL string, moduleName string) string {
	var segments []string
	outputURL = path.Dir(outputURL)
	for len(outputURL) > 1 && outputURL != modulePath {
		fmt.Printf("ModulePath: %v | OutputPath: %v\n", modulePath, outputURL)

		name := path.Base(outputURL)
		segments = append([]string{name}, segments...)
		outputURL = path.Dir(outputURL)
	}

	URL := url.Join(moduleName, segments...)
	fmt.Printf("Generated URL: %v\n", URL)
	return URL
}

func (s *Builder) generateGoFileContent(name string, rType reflect.Type, routeOption *option.RouteConfig, aConfig *ViewConfig, xDatlyModURL *bundleMetadata, packageName string) ([]byte, error) {
	if packageName == "" {
		base := path.Base(aConfig.fileName)
		ext := path.Ext(base)
		packageName = strings.Replace(base, ext, "", 1)
	}

	sbBefore := &bytes.Buffer{}
	sbBefore.WriteString(fmt.Sprintf("var PackageName = \"%v\"\n", packageName))

	var imports xreflect.Imports
	if xDatlyModURL == nil {
		sbBefore.WriteString(fmt.Sprintf(`
var %v = map[string]reflect.Type{
		"%v": reflect.TypeOf(%v{}),
}
`, dConfig.TypesName, name, name))

		imports = append(imports, "reflect")
		packageName = "main"
	} else {
		imports = append(imports,
			moduleCoreTypes,
			path.Join(xDatlyModURL.moduleName, checksumDirectory),
			"reflect",
		)

		sbBefore.WriteString(fmt.Sprintf(`
func init() {
	core.RegisterType(PackageName, "%v", reflect.TypeOf(%v{}), %v.GeneratedTime)
}
`, name, name, checksumDirectory))
	}

	sb := &bytes.Buffer{}
	generatedStruct := xreflect.GenerateStruct(name, rType, imports, xreflect.AppendBeforeType(sbBefore.String()), xreflect.PackageName(packageName))
	sb.WriteString(generatedStruct)
	sb.WriteString("\n")
	source, err := goFormat.Source(sb.Bytes())
	if err != nil {
		return nil, fmt.Errorf("faield to generate go code: %w, %s", err, sb.Bytes())
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
