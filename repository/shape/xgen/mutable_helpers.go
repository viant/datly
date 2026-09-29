package xgen

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/view/state"
)

type mutableComponentSupport struct {
	BodyFieldName string
	BodyTypeName  string
	BodyMany      bool
	Helpers       []mutableIndexHelper
}

type mutableIndexHelper struct {
	ViewParamName string
	ViewFieldName string
	TypeName      string
	MapFieldName  string
	ItemTypeExpr  string
	MapTypeExpr   string
	KeyFieldName  string
	KeyFieldType  string
	KeyReadExpr   string
	NeedNilCheck  bool
	ItemIsPointer bool
	RelationPath  string
	ItemStruct    reflect.Type
}

func (g *ComponentCodegen) mutableSupport(inputType reflect.Type) *mutableComponentSupport {
	if !g.componentUsesVelty() || g.componentUsesHandler() || g.Component == nil || inputType == nil {
		return nil
	}
	bodyFieldName := ""
	for _, input := range g.Component.Input {
		if input == nil || input.In == nil || input.In.Kind != state.KindRequestBody {
			continue
		}
		bodyFieldName = exportedCodegenParamName(input.Name)
		break
	}
	if bodyFieldName == "" {
		return nil
	}

	support := &mutableComponentSupport{BodyFieldName: bodyFieldName}
	for _, input := range g.Component.Input {
		if input == nil || input.In == nil || input.In.Kind != state.KindRequestBody {
			continue
		}
		if input.Schema != nil {
			if bodyTypeName := strings.TrimSpace(input.Schema.Name); bodyTypeName != "" {
				support.BodyTypeName = bodyTypeName
			}
			support.BodyMany = input.Schema.Cardinality == state.Many
		}
		break
	}
	for _, input := range g.mutableHelperParametersForCodegen() {
		if input == nil || input.In == nil || input.In.Kind != state.KindView {
			continue
		}
		helper, ok := g.mutableIndexHelper(inputType, bodyFieldName, input)
		if !ok {
			continue
		}
		support.Helpers = append(support.Helpers, helper)
	}
	if len(support.Helpers) == 0 {
		return nil
	}
	return support
}

func (g *ComponentCodegen) mutableIndexHelper(inputType reflect.Type, bodyFieldName string, param *state.Parameter) (mutableIndexHelper, bool) {
	fieldName := exportedCodegenParamName(param.Name)
	if fieldName == "" {
		return mutableIndexHelper{}, false
	}
	viewField, ok := inputType.FieldByName(fieldName)
	if !ok {
		return mutableIndexHelper{}, false
	}
	itemTypeExpr, itemStructType := collectionItemType(viewField)
	if itemStructType == nil {
		return mutableIndexHelper{}, false
	}
	itemIsPointer := viewField.Type.Kind() == reflect.Slice && viewField.Type.Elem().Kind() == reflect.Ptr
	if namedItemTypeExpr := generatedMutableItemTypeExpr(param, itemIsPointer); namedItemTypeExpr != "" {
		itemTypeExpr = namedItemTypeExpr
	}
	keyFieldName := ""
	keyType := reflect.Type(nil)
	keyReadExpr := ""
	needNilCheck := false
	if g != nil {
		if g.Resource != nil {
			if inputView := lookupInputView(g.Resource, strings.TrimSpace(param.Name)); inputView != nil {
				if _, resolvedFieldName, resolvedType, ok := g.generatedIndexColumn(g.semanticView(inputView)); ok {
					keyFieldName = resolvedFieldName
					keyType = resolvedType
					keyReadExpr = fmt.Sprintf("item.%s", keyFieldName)
					if keyType.Kind() == reflect.Ptr {
						needNilCheck = true
						keyReadExpr = "*" + keyReadExpr
						keyType = keyType.Elem()
					}
				}
			}
		}
		if keyType == nil {
			if resourceType := g.resourceViewStructType(strings.TrimSpace(param.Name)); resourceType != nil {
				if keyField, ok := lookupGeneratedIndexField(resourceType); ok {
					keyFieldName = keyField.Name
					keyType = keyField.Type
					keyReadExpr = fmt.Sprintf("item.%s", keyFieldName)
					if keyType.Kind() == reflect.Ptr {
						needNilCheck = true
						keyReadExpr = "*" + keyReadExpr
						keyType = keyType.Elem()
					}
				}
			}
		}
	}
	if keyType == nil {
		keyField, ok := lookupGeneratedIndexField(itemStructType)
		if !ok {
			return mutableIndexHelper{}, false
		}
		keyFieldName = keyField.Name
		keyType = keyField.Type
		keyReadExpr = fmt.Sprintf("item.%s", keyFieldName)
		if keyType.Kind() == reflect.Ptr {
			needNilCheck = true
			keyReadExpr = "*" + keyReadExpr
			keyType = keyType.Elem()
		}
	}
	keyTypeExpr := sourceTypeExpr(keyType, "")
	if keyTypeExpr == "" {
		return mutableIndexHelper{}, false
	}
	mapFieldName := fieldName + "By" + keyFieldName
	if _, exists := inputType.FieldByName(mapFieldName); exists {
		return mutableIndexHelper{}, false
	}
	return mutableIndexHelper{
		ViewParamName: strings.TrimSpace(param.Name),
		ViewFieldName: fieldName,
		TypeName: func() string {
			if param.Schema == nil {
				return ""
			}
			return strings.TrimSpace(param.Schema.Name)
		}(),
		MapFieldName:  mapFieldName,
		ItemTypeExpr:  itemTypeExpr,
		MapTypeExpr:   fmt.Sprintf("map[%s]%s", keyTypeExpr, itemTypeExpr),
		KeyFieldName:  keyFieldName,
		KeyFieldType:  keyTypeExpr,
		KeyReadExpr:   keyReadExpr,
		NeedNilCheck:  needNilCheck,
		ItemIsPointer: itemIsPointer,
		RelationPath:  mutableRelationPath(inputType, itemStructType, bodyFieldName),
		ItemStruct:    itemStructType,
	}, true
}

func generatedMutableItemTypeExpr(param *state.Parameter, itemIsPointer bool) string {
	if param == nil || param.Schema == nil {
		return ""
	}
	typeName := strings.TrimSpace(param.Schema.Name)
	if typeName == "" {
		return ""
	}
	if itemIsPointer {
		return "*" + typeName
	}
	return typeName
}

func (g *ComponentCodegen) mutableHelperParametersForCodegen() []*state.Parameter {
	params := g.codegenInputParameters()
	if len(params) == 0 {
		return nil
	}
	result := make([]*state.Parameter, 0, len(params))
	for _, item := range params {
		if item == nil {
			continue
		}
		result = append(result, item)
	}
	return result
}

func mutableRelationPath(inputType reflect.Type, itemType reflect.Type, bodyFieldName string) string {
	if inputType == nil || itemType == nil || bodyFieldName == "" {
		return ""
	}
	bodyField, ok := inputType.FieldByName(bodyFieldName)
	if !ok {
		return ""
	}
	rootType, _ := mutableBodyItemType(bodyField.Type)
	if rootType == nil {
		return ""
	}
	if sameNamedStructType(rootType, itemType) {
		return ""
	}
	return lookupMutableRelationPath(rootType, itemType, "")
}

func lookupMutableRelationPath(parentType reflect.Type, itemType reflect.Type, prefix string) string {
	parentType = unwrapNamedStructType(parentType)
	itemType = unwrapNamedStructType(itemType)
	if parentType == nil || itemType == nil {
		return ""
	}
	for i := 0; i < parentType.NumField(); i++ {
		field := parentType.Field(i)
		if !isMutableRelationField(field) {
			continue
		}
		childType, _ := mutableBodyItemType(field.Type)
		if childType == nil {
			continue
		}
		current := field.Name
		if prefix != "" {
			current = prefix + "/" + current
		}
		if sameNamedStructType(childType, itemType) {
			return current
		}
		if nested := lookupMutableRelationPath(childType, itemType, current); nested != "" {
			return nested
		}
	}
	return ""
}

func sameNamedStructType(left, right reflect.Type) bool {
	left = unwrapNamedStructType(left)
	right = unwrapNamedStructType(right)
	if left == nil || right == nil {
		return false
	}
	if left == right {
		return true
	}
	if left.Name() != "" && right.Name() != "" && left.Name() == right.Name() && left.PkgPath() == right.PkgPath() {
		return true
	}
	return false
}

func (s *mutableComponentSupport) renderInputFields(builder *strings.Builder) {
	if s == nil {
		return
	}
	for _, helper := range s.Helpers {
		builder.WriteString(fmt.Sprintf("\t%s %s `json:\"-\"`\n", helper.MapFieldName, helper.MapTypeExpr))
	}
}

func (s *mutableComponentSupport) renderInputInit(inputTypeName, outputTypeName string) string {
	if s == nil {
		return ""
	}
	if strings.TrimSpace(inputTypeName) == "" {
		inputTypeName = "Input"
	}
	if strings.TrimSpace(outputTypeName) == "" {
		outputTypeName = "Output"
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("func (i *%s) Init(ctx context.Context, sess handler.Session, output *%s) error {\n", inputTypeName, outputTypeName))
	builder.WriteString("\tif err := sess.Stater().Bind(ctx, i); err != nil {\n")
	builder.WriteString("\t\treturn err\n")
	builder.WriteString("\t}\n")
	builder.WriteString("\ti.indexSlice()\n")
	builder.WriteString("\treturn nil\n")
	builder.WriteString("}\n\n")
	builder.WriteString(fmt.Sprintf("func (i *%s) indexSlice() {\n", inputTypeName))
	for _, helper := range s.Helpers {
		builder.WriteString(fmt.Sprintf("\ti.%s = make(%s, len(i.%s))\n", helper.MapFieldName, helper.MapTypeExpr, helper.ViewFieldName))
		builder.WriteString(fmt.Sprintf("\tfor _, item := range i.%s {\n", helper.ViewFieldName))
		if helper.ItemIsPointer {
			builder.WriteString("\t\tif item == nil {\n")
			builder.WriteString("\t\t\tcontinue\n")
			builder.WriteString("\t\t}\n")
		}
		if helper.NeedNilCheck {
			builder.WriteString(fmt.Sprintf("\t\tif item.%s == nil {\n", helper.KeyFieldName))
			builder.WriteString("\t\t\tcontinue\n")
			builder.WriteString("\t\t}\n")
		}
		builder.WriteString(fmt.Sprintf("\t\ti.%s[%s] = item\n", helper.MapFieldName, helper.KeyReadExpr))
		builder.WriteString("\t}\n")
	}
	builder.WriteString("}\n")
	return builder.String()
}

func (s *mutableComponentSupport) renderInputValidate(inputTypeName, outputTypeName string) string {
	if s == nil {
		return ""
	}
	if strings.TrimSpace(inputTypeName) == "" {
		inputTypeName = "Input"
	}
	if strings.TrimSpace(outputTypeName) == "" {
		outputTypeName = "Output"
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("func (i *%s) Validate(ctx context.Context, sess handler.Session, output *%s) error {\n", inputTypeName, outputTypeName))
	builder.WriteString("\taValidator := sess.Validator()\n")
	builder.WriteString("\tsessionDb, err := sess.Db()\n")
	builder.WriteString("\tif err != nil {\n")
	builder.WriteString("\t\treturn err\n")
	builder.WriteString("\t}\n")
	builder.WriteString("\tdb, err := sessionDb.Db(ctx)\n")
	builder.WriteString("\tif err != nil {\n")
	builder.WriteString("\t\treturn err\n")
	builder.WriteString("\t}\n")
	builder.WriteString("\tvar options = []validator.Option{\n")
	builder.WriteString(fmt.Sprintf("\t\tvalidator.WithLocation(%q),\n", s.BodyFieldName))
	builder.WriteString("\t\tvalidator.WithDB(db),\n")
	builder.WriteString("\t\tvalidator.WithUnique(true),\n")
	builder.WriteString("\t\tvalidator.WithRefCheck(true),\n")
	builder.WriteString("\t\tvalidator.WithCanUseMarkerProvider(i.canUseMarkerProvider),\n")
	builder.WriteString("\t}\n")
	builder.WriteString("\tvalidation := validator.NewValidation()\n")
	builder.WriteString(fmt.Sprintf("\terr = i.validate(ctx, aValidator, validation, options, i.%s)\n", s.BodyFieldName))
	builder.WriteString("\toutput.Violations = append(output.Violations, validation.Violations...)\n")
	builder.WriteString("\tif err == nil && len(validation.Violations) > 0 {\n")
	builder.WriteString("\t\tvalidation.Violations.Sort()\n")
	builder.WriteString("\t}\n")
	builder.WriteString("\treturn err\n")
	builder.WriteString("}\n\n")
	builder.WriteString(fmt.Sprintf("func (i *%s) validate(ctx context.Context, aValidator *validator.Service, validation *validator.Validation, options []validator.Option, value interface{}) error {\n", inputTypeName))
	builder.WriteString("\t_, err := aValidator.Validate(ctx, value, append(options, validator.WithValidation(validation))...)\n")
	builder.WriteString("\tif err != nil {\n")
	builder.WriteString("\t\treturn err\n")
	builder.WriteString("\t}\n")
	builder.WriteString("\treturn nil\n")
	builder.WriteString("}\n\n")
	builder.WriteString(fmt.Sprintf("func (i *%s) canUseMarkerProvider(v interface{}) bool {\n", inputTypeName))
	builder.WriteString("\tswitch actual := v.(type) {\n")
	for _, helper := range s.Helpers {
		builder.WriteString(fmt.Sprintf("\tcase %s:\n", helper.ItemTypeExpr))
		if helper.NeedNilCheck {
			builder.WriteString(fmt.Sprintf("\t\tif actual.%s == nil {\n", helper.KeyFieldName))
			builder.WriteString("\t\t\treturn false\n")
			builder.WriteString("\t\t}\n")
		}
		actualKey := fmt.Sprintf("actual.%s", helper.KeyFieldName)
		if helper.NeedNilCheck {
			actualKey = "*" + actualKey
		}
		builder.WriteString(fmt.Sprintf("\t\t_, ok := i.%s[%s]\n", helper.MapFieldName, actualKey))
		builder.WriteString("\t\treturn ok\n")
	}
	builder.WriteString("\tdefault:\n")
	builder.WriteString("\t\treturn true\n")
	builder.WriteString("\t}\n")
	builder.WriteString("}\n")
	return builder.String()
}

func collectionItemType(field reflect.StructField) (string, reflect.Type) {
	rType := field.Type
	expr := sourceFieldTypeExpr(field)
	if expr == "" || rType == nil {
		return "", nil
	}
	switch rType.Kind() {
	case reflect.Slice, reflect.Array:
		return strings.TrimPrefix(expr, "[]"), unwrapNamedStructType(rType.Elem())
	default:
		return "", nil
	}
}

func unwrapNamedStructType(rType reflect.Type) reflect.Type {
	for rType != nil && rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	if rType == nil || rType.Kind() != reflect.Struct {
		return nil
	}
	return rType
}

func lookupGeneratedIndexField(structType reflect.Type) (reflect.StructField, bool) {
	if structType == nil || structType.Kind() != reflect.Struct {
		return reflect.StructField{}, false
	}
	if field, ok := structType.FieldByName("Id"); ok {
		return field, true
	}
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if generatedSQLXFieldName(field.Tag.Get("sqlx")) == "ID" {
			return field, true
		}
	}
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if strings.Contains(strings.ToLower(field.Tag.Get("sqlx")), "primarykey") {
			return field, true
		}
	}
	return reflect.StructField{}, false
}

func sourceTypeExpr(rType reflect.Type, typeName string) string {
	if rType == nil {
		return typeName
	}
	switch rType.Kind() {
	case reflect.Ptr:
		return "*" + sourceTypeExpr(rType.Elem(), typeName)
	case reflect.Slice:
		return "[]" + sourceTypeExpr(rType.Elem(), typeName)
	case reflect.Array:
		return fmt.Sprintf("[%d]%s", rType.Len(), sourceTypeExpr(rType.Elem(), typeName))
	case reflect.Map:
		return "map[" + sourceTypeExpr(rType.Key(), "") + "]" + sourceTypeExpr(rType.Elem(), typeName)
	default:
		if typeName != "" {
			return typeName
		}
		return rType.String()
	}
}

func generatedSQLXFieldName(tag string) string {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return ""
	}
	for _, part := range strings.Split(tag, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.HasPrefix(part, "name=") {
			return strings.TrimSpace(strings.TrimPrefix(part, "name="))
		}
		if !strings.Contains(part, "=") {
			return part
		}
	}
	return ""
}
