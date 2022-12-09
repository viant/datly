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

func (m *inputMetadata) primaryKeyFields() []*view.Field {
	var pkFields []*view.Field
	for i, field := range m.actualFields {
		meta, ok := m.meta.metaByColName(field.Column)
		if !ok || !meta.primaryKey {
			continue
		}

		pkFields = append(pkFields, m.actualFields[i])
	}

	return pkFields
}

func (s *Builder) preparePostRule(ctx context.Context, sourceSQL []byte) (string, error) {
	routeOption, config, paramType, err := s.buildInputMetadata(ctx, sourceSQL)
	if err != nil {
		return "", err
	}

	template, err := s.buildInsertSQL(paramType, config, routeOption)
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
	requiredTypes := []string{"*" + typeDef.paramName}

	routeOption.RequestBody = &option.BodyConfig{
		DataType: typeDef.bodyHolder,
	}

	routeOption.ResponseBody = &option.ResponseBodyConfig{
		From: paramName,
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

func (isb *insertStmtBuilder) appendAllocation(def *inputMetadata, path, holderName string) {
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

func (s *Builder) buildRequestBodyPostParam(config *viewConfig, def *inputMetadata) (reflect.Type, error) {
	if err := def.typeDef.Init(context.Background(), map[string]reflect.Type{}); err != nil {
		return nil, err
	}

	return def.typeDef.Schema.Type(), nil
}

func newInsertStmtBuilder(sb *strings.Builder, def *inputMetadata) *insertStmtBuilder {
	return &insertStmtBuilder{
		stmtBuilder: newStmtBuilder(sb, def, ""),
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
	if err := isb.stmtBuilder.appendColumnNames(isb.accessParam(indirectParent, name, false)); err != nil {
		return "", err
	}

	//for i, field := range isb.typeDef.actualFields {
	//	if i != 0 {
	//		isb.writeString(",\n")
	//	}
	//	isb.writeString(field.Column)
	//}

	isb.writeString("\n) VALUES (")
	if err := isb.appendColumnValues(isb.accessParam(indirectParent, name, false)); err != nil {
		return "", err
	}
	//for i, field := range isb.typeDef.actualFields {
	//	if i != 0 {
	//		isb.writeString(",\n")
	//	}
	//	isb.writeString("$")
	//	isb.writeString(isb.accessParam(indirectParent, name, false))
	//	isb.writeString(".")
	//	isb.writeString(field.Name)
	//	if err := isb.tryWriteParamHint(); err != nil {
	//		return "", err
	//	}
	//}
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

func (isb *insertStmtBuilder) newRelation(rel *inputMetadata) *insertStmtBuilder {
	builder := newInsertStmtBuilder(isb.sb, rel)
	builder.indent = isb.indent
	if builder.isMulti {
		builder.indent += "	"
	}
	builder.parent = isb
	builder.wroteHint = isb.wroteHint
	return builder
}
