package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

type (
	inputMetadata struct {
		typeDef      *view.Definition
		meta         *typeMeta
		actualFields []*view.Field
		bodyHolder   string
		paramName    string
		relations    []*inputMetadata
		fkIndex      map[string]sink.Key
		pkIndex      map[string]sink.Key
		table        string
		config       *viewConfig
		sql          string
		query        string
		sqlName      string
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
		parent *insertStmtBuilder
		*stmtBuilder
	}
)

func (s *Builder) preparePostRule(ctx context.Context, sourceSQL []byte) (string, error) {
	routeOption, config, paramType, err := s.buildInputMetadata(ctx, sourceSQL)
	if err != nil {
		return "", err
	}

	template, err := s.buildInsertSQL(paramType, config, routeOption)
	if err != nil {
		return "", err
	}

	if _, err = s.uploadSQL(folderSQL, s.fileNames.unique(config.fileName), template, false); err != nil {
		return "", nil
	}

	return template, nil
}

func (s *Builder) extractRouteSettings(sourceSQL []byte) (string, string) {
	hint := sanitize.ExtractHint(string(sourceSQL))
	SQL := strings.Replace(string(sourceSQL), hint, "", 1)
	return hint, SQL
}

func (s *Builder) uploadGoType(name string, rType reflect.Type) error {
	fileContent := xreflect.GenerateStruct(name, rType)
	if _, err := s.uploadGo(folderSQL, name, fileContent, false); err != nil {
		return err
	}

	sampleValue := getStruct(rType)
	sample := sampleValue.Interface()
	if data, err := json.Marshal(sample); err == nil {
		s.uploadFile(folderSQL, name+"Post", string(data), false, ".json")
	}

	return nil
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

func (s *Builder) buildInsertSQL(typeDef *inputMetadata, config *viewConfig, routeOption *option.RouteConfig) (string, error) {
	sb, err := s.prepareStringBuilder(typeDef, config, routeOption)
	if err != nil {
		return "", err
	}

	builder := newInsertStmtBuilder(sb, typeDef)
	if err := builder.appendHintsWithRelations(); err != nil {
		return "", err
	}

	builder.appendAllocation(typeDef, "", typeDef.paramName)

	return builder.build("", true)
}

func (s *Builder) prepareStringBuilder(typeDef *inputMetadata, config *viewConfig, routeOption *option.RouteConfig) (*strings.Builder, error) {
	sb := &strings.Builder{}
	typeName := typeDef.typeDef.Name

	paramType, err := s.buildRequestBodyPostParam(config, typeDef)
	if err != nil {
		return nil, err
	}

	if err = s.uploadGoType(typeName, paramType); err != nil {
		return nil, err
	}

	if err = s.appendPostRouteOption(typeDef.paramName, routeOption, typeName, typeDef, sb); err != nil {
		return nil, err
	}
	return sb, nil
}

func (s *Builder) appendPostRouteOption(paramName string, routeOption *option.RouteConfig, typeName string, typeDef *inputMetadata, sb *strings.Builder) error {

	routeOption.RequestBody = &option.BodyConfig{
		DataType: typeDef.bodyHolder,
	}

	routeOption.ResponseBody = &option.ResponseBodyConfig{
		From: paramName,
	}

	routeOption.Declare = map[string]string{}

	routeOption.Declare[view.FirstNotEmpty(typeDef.bodyHolder, typeDef.paramName)] = typeName
	marshal, err := json.Marshal(routeOption)
	if err != nil {
		return err
	}

	if routeJSON := string(marshal); routeJSON != "{}" {
		sb.WriteString(fmt.Sprintf("/* %v */\n\n", routeJSON))
	}

	requiredTypes := []string{typeDef.paramName}
	if typeDef.bodyHolder != "" {
		requiredTypes = append(requiredTypes, typeDef.bodyHolder)
	}

	if len(requiredTypes) > 0 {
		sb.WriteString("import (")
		for _, requiredType := range requiredTypes {
			sb.WriteString(fmt.Sprintf("\n	\"%v.%v\"", folderSQL, requiredType))
		}
		sb.WriteString("\n)\n\n")
	}

	return nil
}

func (isb *insertStmtBuilder) appendAllocation(def *inputMetadata, path, holderName string) {
	for _, meta := range def.meta.metas {
		if !meta.autoincrement {
			continue
		}

		isb.writeString("\n")
		isb.writeString(fmt.Sprintf(`$sequencer.Allocate("%v", $%v, "%v")`, def.table, holderName, path+meta.fieldName))
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

func (s *Builder) buildRequestBodyPostParam(config *viewConfig, def *inputMetadata) (reflect.Type, error) {
	if err := def.typeDef.Init(context.Background(), map[string]reflect.Type{}); err != nil {
		return nil, err
	}

	return def.typeDef.Schema.Type(), nil
}

func newInsertStmtBuilder(sb *strings.Builder, def *inputMetadata) *insertStmtBuilder {
	return &insertStmtBuilder{
		stmtBuilder: newStmtBuilder(sb, def),
	}
}

func (isb *insertStmtBuilder) build(parentRecord string, withUnsafe bool) (string, error) {
	accessor, ok := isb.appendForEachIfNeeded(parentRecord, isb.paramName, withUnsafe)
	contentBuilder := isb
	if ok {
		contentBuilder = contentBuilder.withIndent()
	}

	withUnsafe = accessor.withUnsafe

	if contentBuilder.parent != nil {
		contentBuilder.appendSetFk(accessor, contentBuilder.parent.stmtBuilder)
	}

	if err := contentBuilder.appendInsert(accessor); err != nil {
		return "", err
	}

	for _, rel := range isb.typeDef.relations {
		_, err := contentBuilder.newRelation(rel).build(accessor.record, !contentBuilder.isMulti && withUnsafe)
		if err != nil {
			return "", err
		}
	}

	if ok {
		isb.writeString("\n#end")
	}
	return isb.sb.String(), nil
}

func (isb *insertStmtBuilder) appendInsert(accessor *paramAccessor) error {
	isb.writeString("\nINSERT INTO ")
	isb.writeString(isb.typeDef.table)
	isb.writeString("( ")
	if err := isb.stmtBuilder.appendColumnNames(accessor, false); err != nil {
		return err
	}

	isb.writeString("\n) VALUES (")
	if err := isb.appendColumnValues(accessor, false); err != nil {
		return err
	}
	isb.writeString("\n);\n")
	return nil
}

func (sb *stmtBuilder) appendSetFk(accessor *paramAccessor, parent *stmtBuilder) {
	if parent != nil {
		for _, meta := range sb.typeDef.meta.metas {
			if meta.fkKey == nil {
				continue
			}

			if meta.fkKey.ReferenceTable != parent.typeDef.table {
				continue
			}

			refMeta, ok := parent.typeDef.meta.metaByColName(meta.fkKey.ReferenceColumn)
			if !ok {
				continue
			}

			sb.writeString(fmt.Sprintf("\n#set($%v.%v = $%v.%v)", accessor.unsafeRecord, meta.fieldName, accessor.unsafeParent, refMeta.fieldName))
		}
	}
}

func (isb *insertStmtBuilder) newRelation(rel *inputMetadata) *insertStmtBuilder {
	builder := isb.stmtBuilder.newRelation(rel)
	return &insertStmtBuilder{
		stmtBuilder: builder,
		parent:      isb,
	}
}

func (isb *insertStmtBuilder) withIndent() *insertStmtBuilder {
	aCopy := *isb
	aCopy.stmtBuilder = aCopy.stmtBuilder.withIndent()
	return &aCopy
}
