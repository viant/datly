package xgen

import (
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	shapeast "github.com/viant/datly/repository/shape/velty/ast"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/tagly/format/text"
)

func (g *ComponentCodegen) renderMutableVeltyBody(inputType reflect.Type) (string, bool, error) {
	if inputType == nil {
		var err error
		inputType, err = g.mutableInputType()
		if err != nil {
			return "", false, err
		}
	}
	support := g.mutableSupport(inputType)
	if support == nil {
		return "", false, nil
	}
	block, err := g.buildMutableVeltyBlock(inputType, support)
	if err != nil {
		return "", false, err
	}
	if block == nil {
		return "", false, nil
	}
	builder := shapeast.NewBuilder(shapeast.Options{Lang: shapeast.LangVelty})
	if err = block.Generate(builder); err != nil {
		return "", false, err
	}
	return strings.TrimSpace(builder.String()) + "\n", true, nil
}

func (g *ComponentCodegen) renderMutableDSQL(inputType reflect.Type) (string, bool, error) {
	body, ok, err := g.renderMutableVeltyBody(inputType)
	if err != nil || !ok {
		return "", ok, err
	}
	support := g.mutableSupport(inputType)
	if support == nil {
		return "", false, nil
	}
	var builder strings.Builder
	builder.WriteString("/* ")
	builder.WriteString(g.mutableRouteOptionJSON())
	builder.WriteString(" */\n\n\n")
	if imports := g.mutableTypeImports(support, inputType); len(imports) > 0 {
		builder.WriteString("import (\n")
		for _, item := range imports {
			builder.WriteString("\t")
			builder.WriteString(strconvQuote(item))
			builder.WriteString("\n")
		}
		builder.WriteString("\t)\n\n\n")
	}
	builder.WriteString(g.mutableBodyDeclaration(inputType, support))
	for _, helper := range g.mutableIDHelpers(support) {
		builder.WriteString(g.mutableIDsDeclaration(helper))
	}
	for _, helper := range g.mutableViewHelpers(support) {
		builder.WriteString(g.mutableViewDeclaration(helper))
	}
	builder.WriteString(g.mutableOutputDeclaration(inputType, support))
	builder.WriteString("\n\n")
	builder.WriteString(body)
	return builder.String(), true, nil
}

func (g *ComponentCodegen) mutableTypeImports(support *mutableComponentSupport, inputType reflect.Type) []string {
	items := map[string]struct{}{}
	add := func(typeName string) {
		typeName = strings.TrimSpace(typeName)
		if typeName == "" {
			return
		}
		pkg := strings.TrimSpace(g.PackageName)
		if pkg == "" && g.TypeContext != nil {
			pkg = strings.TrimSpace(g.TypeContext.PackageName)
		}
		if pkg == "" {
			return
		}
		items[pkg+"."+typeName] = struct{}{}
	}
	if bodyField, ok := inputType.FieldByName(support.BodyFieldName); ok {
		if itemType, _ := mutableBodyItemType(bodyField.Type); itemType != nil {
			typeName := strings.TrimSpace(support.BodyTypeName)
			if typeName == "" {
				typeName = itemType.Name()
			}
			add(typeName)
		}
	}
	for _, helper := range support.Helpers {
		typeName := strings.TrimSpace(helper.TypeName)
		if typeName == "" && helper.ItemStruct != nil {
			typeName = helper.ItemStruct.Name()
		}
		if typeName != "" {
			add(typeName)
		}
	}
	result := make([]string, 0, len(items))
	for item := range items {
		result = append(result, item)
	}
	sort.Strings(result)
	return result
}

func (g *ComponentCodegen) mutableRouteOptionJSON() string {
	connector := g.rootConnectorRef()
	parts := []string{
		`"URI":"` + escapeJSON(strings.TrimSpace(g.Component.URI)) + `"`,
		`"Method":"` + escapeJSON(strings.ToUpper(strings.TrimSpace(g.Component.Method))) + `"`,
	}
	if connector != "" {
		parts = append(parts, `"Connector":"`+escapeJSON(connector)+`"`)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func (g *ComponentCodegen) mutableBodyDeclaration(inputType reflect.Type, support *mutableComponentSupport) string {
	bodyField, ok := inputType.FieldByName(support.BodyFieldName)
	if !ok {
		return ""
	}
	itemType, many := mutableBodyItemType(bodyField.Type)
	if itemType == nil {
		return ""
	}
	typeName := strings.TrimSpace(support.BodyTypeName)
	if typeName == "" {
		typeName = itemType.Name()
	}
	typeExpr := typeName
	if many {
		typeExpr = "[]" + typeName
	}
	return "#set($_ = $" + support.BodyFieldName + "<" + typeExpr + ">(body/).WithTag('anonymous:\"true\"').Required())\n"
}

func (g *ComponentCodegen) mutableIDsDeclaration(helper mutableIndexHelper) string {
	paramName := g.mutableIDsParamName(helper)
	sqlText := g.mutableIDSQL(helper)
	if paramName == "" || sqlText == "" {
		return ""
	}
	return "\t#set($_ = $" + paramName + "<?>(param/" + g.supportBodyFieldName(helper) + ") /*\n" + sqlText + "\n*/\n)\n"
}

func (g *ComponentCodegen) mutableViewDeclaration(helper mutableIndexHelper) string {
	typeName := strings.TrimSpace(helper.TypeName)
	if typeName == "" && helper.ItemStruct != nil {
		typeName = strings.TrimSpace(helper.ItemStruct.Name())
	}
	viewType := "[]*" + typeName
	if typeName == "" || helper.ViewFieldName == "" {
		return ""
	}
	sqlText := g.mutableDeclarationViewSQL(helper)
	if sqlText == "" {
		return ""
	}
	return "\t#set($_ = $" + helper.ViewFieldName + "<" + viewType + ">(view/" + helper.ViewFieldName + ") /*\n" + sqlText + "\n*/\n)\n"
}

func (g *ComponentCodegen) mutableOutputDeclaration(inputType reflect.Type, support *mutableComponentSupport) string {
	bodyField, ok := inputType.FieldByName(support.BodyFieldName)
	if !ok {
		return ""
	}
	_, many := mutableBodyItemType(bodyField.Type)
	typeExpr := ""
	if many {
		typeExpr = "[]"
	}
	typeName := strings.TrimSpace(support.BodyTypeName)
	if typeName == "" {
		if itemType, _ := mutableBodyItemType(bodyField.Type); itemType != nil {
			typeName = itemType.Name()
		}
	}
	tag := `anonymous:"true"`
	if typeName != "" {
		tag += `  typeName:"` + typeName + `"`
	}
	return "#set($_ = $" + support.BodyFieldName + "<" + typeExpr + ">(body/).WithTag('" + tag + "').Required().Output())\n"
}

func (g *ComponentCodegen) mutableIDSQL(helper mutableIndexHelper) string {
	key := strings.TrimSpace(helper.KeyFieldName)
	if key == "" {
		key = "Id"
	}
	path := "/"
	if helper.RelationPath != "" {
		path += helper.RelationPath
	}
	return "? SELECT ARRAY_AGG(" + key + ") AS Values FROM  `" + path + "` LIMIT 1"
}

func (g *ComponentCodegen) mutableViewSQL(helper mutableIndexHelper) string {
	if g == nil || g.Resource == nil {
		return g.mutableFallbackViewSQL(helper)
	}
	for _, aView := range g.Resource.Views {
		if aView == nil || !strings.EqualFold(strings.TrimSpace(aView.Name), strings.TrimSpace(helper.ViewParamName)) {
			continue
		}
		if aView.Template == nil {
			return g.mutableFallbackViewSQL(helper)
		}
		sqlText := strings.TrimSpace(aView.Template.Source)
		if sqlText != "" {
			return sqlText
		}
		return g.mutableFallbackViewSQL(helper)
	}
	return g.mutableFallbackViewSQL(helper)
}

func (g *ComponentCodegen) mutableDeclarationViewSQL(helper mutableIndexHelper) string {
	sqlText := strings.TrimSpace(g.mutableViewSQL(helper))
	if sqlText == "" {
		return ""
	}
	if strings.HasPrefix(sqlText, "?") {
		return sqlText
	}
	return "? " + sqlText
}

func (g *ComponentCodegen) mutableFallbackViewSQL(helper mutableIndexHelper) string {
	tableName := ""
	if helper.ItemStruct != nil && helper.ItemStruct.Name() != "" {
		tableName = tableNameFromType(helper.ItemStruct.Name())
	}
	if tableName == "" {
		typeName := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(helper.ItemTypeExpr), "[]"), "*")
		tableName = tableNameFromType(typeName)
	}
	if tableName == "" {
		return ""
	}
	idParam := g.mutableIDsParamName(helper)
	key := strings.TrimSpace(helper.KeyFieldName)
	if key == "" {
		key = "Id"
	}
	return "SELECT * FROM " + tableName + "\nWHERE $criteria.In(\"" + key + "\", $" + idParam + ".Values)"
}

func (g *ComponentCodegen) supportBodyFieldName(helper mutableIndexHelper) string {
	if g == nil {
		return ""
	}
	inputType, err := g.mutableInputType()
	if err != nil || inputType == nil {
		return ""
	}
	if support := g.mutableSupport(inputType); support != nil {
		return support.BodyFieldName
	}
	return ""
}

func strconvQuote(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"`
}

func escapeJSON(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}

func (g *ComponentCodegen) mutableIDHelpers(support *mutableComponentSupport) []mutableIndexHelper {
	if support == nil || len(support.Helpers) == 0 {
		return nil
	}
	ret := append([]mutableIndexHelper{}, support.Helpers...)
	sort.SliceStable(ret, func(i, j int) bool {
		leftDepth := mutableRelationDepth(ret[i].RelationPath)
		rightDepth := mutableRelationDepth(ret[j].RelationPath)
		if leftDepth != rightDepth {
			return leftDepth < rightDepth
		}
		return ret[i].ViewFieldName < ret[j].ViewFieldName
	})
	return ret
}

func (g *ComponentCodegen) mutableViewHelpers(support *mutableComponentSupport) []mutableIndexHelper {
	if support == nil || len(support.Helpers) == 0 {
		return nil
	}
	ret := append([]mutableIndexHelper{}, support.Helpers...)
	sort.SliceStable(ret, func(i, j int) bool {
		leftDepth := mutableRelationDepth(ret[i].RelationPath)
		rightDepth := mutableRelationDepth(ret[j].RelationPath)
		if leftDepth != rightDepth {
			return leftDepth > rightDepth
		}
		return ret[i].ViewFieldName > ret[j].ViewFieldName
	})
	return ret
}

func mutableRelationDepth(path string) int {
	path = strings.Trim(path, "/")
	if path == "" {
		return 0
	}
	return strings.Count(path, "/") + 1
}

type mutableGeneratedFile struct {
	Path    string
	Content string
}

func (g *ComponentCodegen) mutableHelperSQLFiles(support *mutableComponentSupport) []mutableGeneratedFile {
	if g == nil || g.Component == nil || support == nil {
		return nil
	}
	packageDir := strings.TrimSpace(g.PackageDir)
	if packageDir == "" && g.TypeContext != nil {
		packageDir = strings.TrimSpace(g.TypeContext.PackageDir)
	}
	if packageDir == "" {
		return nil
	}
	var result []mutableGeneratedFile
	helperByView := map[string]mutableIndexHelper{}
	helperByID := map[string]mutableIndexHelper{}
	for _, helper := range support.Helpers {
		helperByView[strings.TrimSpace(helper.ViewFieldName)] = helper
		helperByID[g.mutableIDsParamName(helper)] = helper
	}
	for _, input := range g.Component.Input {
		if input == nil {
			continue
		}
		switch {
		case input.In != nil && input.In.Kind == state.KindView:
			helper, ok := helperByView[strings.TrimSpace(input.Name)]
			if !ok {
				continue
			}
			rel := tagURIValue(input.Tag, "sql")
			content := g.mutableViewSQL(helper)
			if strings.TrimSpace(content) == "" {
				continue
			}
			result = append(result, g.mutableGeneratedSQLFiles(packageDir, rel, g.mutableHelperViewRelPath(helper), content)...)
		case input.In != nil && input.In.Kind == state.KindParam:
			helper, ok := helperByID[strings.TrimSpace(input.Name)]
			if !ok {
				continue
			}
			rel := tagURIValue(input.Tag, "codec")
			content := g.mutableIDSQL(helper)
			if strings.TrimSpace(content) == "" {
				continue
			}
			result = append(result, g.mutableGeneratedSQLFiles(packageDir, rel, g.mutableHelperIDsRelPath(helper), content)...)
		}
	}
	for _, helper := range support.Helpers {
		if content := g.mutableViewSQL(helper); strings.TrimSpace(content) != "" {
			result = append(result, g.mutableGeneratedSQLFiles(packageDir, "", g.mutableHelperViewRelPath(helper), content)...)
		}
		if content := g.mutableIDSQL(helper); strings.TrimSpace(content) != "" {
			result = append(result, g.mutableGeneratedSQLFiles(packageDir, "", g.mutableHelperIDsRelPath(helper), content)...)
		}
	}
	return result
}

func (g *ComponentCodegen) mutableGeneratedSQLFiles(packageDir, primaryRel, fallbackRel, content string) []mutableGeneratedFile {
	seen := map[string]struct{}{}
	var result []mutableGeneratedFile
	appendFile := func(rel string) {
		rel = strings.TrimSpace(rel)
		if rel == "" {
			return
		}
		abs := filepath.Join(packageDir, filepath.FromSlash(rel))
		if _, ok := seen[abs]; ok {
			return
		}
		seen[abs] = struct{}{}
		result = append(result, mutableGeneratedFile{Path: abs, Content: content})
	}
	appendFile(primaryRel)
	appendFile(fallbackRel)
	return result
}

func tagURIValue(tag, key string) string {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return ""
	}
	needle := key + `:"`
	start := strings.Index(tag, needle)
	if start == -1 {
		return ""
	}
	rest := tag[start+len(needle):]
	end := strings.Index(rest, `"`)
	if end == -1 {
		return ""
	}
	value := rest[:end]
	if idx := strings.Index(value, "uri="); idx >= 0 {
		value = value[idx+4:]
		if cut := strings.IndexAny(value, ", "); cut >= 0 {
			value = value[:cut]
		}
	}
	return strings.TrimSpace(value)
}

func (g *ComponentCodegen) mutableIDsParamName(helper mutableIndexHelper) string {
	name := "Cur"
	support := g.mutableSupportMust()
	if support != nil {
		name += support.BodyFieldName
	}
	if helper.RelationPath != "" {
		name += strings.ReplaceAll(helper.RelationPath, "/", "")
	}
	name += helper.KeyFieldName
	return name
}

func (g *ComponentCodegen) mutableHelperViewRelPath(helper mutableIndexHelper) string {
	componentDir := text.CaseFormatUpperCamel.Format(strings.TrimSpace(g.componentName()), text.CaseFormatLowerUnderscore)
	name := text.CaseFormatUpperCamel.Format(strings.TrimSpace(helper.ViewFieldName), text.CaseFormatLowerUnderscore)
	if componentDir == "" || name == "" {
		return ""
	}
	return filepath.ToSlash(filepath.Join(componentDir, name+".sql"))
}

func (g *ComponentCodegen) mutableHelperIDsRelPath(helper mutableIndexHelper) string {
	componentDir := text.CaseFormatUpperCamel.Format(strings.TrimSpace(g.componentName()), text.CaseFormatLowerUnderscore)
	name := text.CaseFormatUpperCamel.Format(strings.TrimSpace(g.mutableIDsParamName(helper)), text.CaseFormatLowerUnderscore)
	if componentDir == "" || name == "" {
		return ""
	}
	return filepath.ToSlash(filepath.Join(componentDir, name+".sql"))
}

func (g *ComponentCodegen) mutableSupportMust() *mutableComponentSupport {
	inputType, err := g.mutableInputType()
	if err != nil || inputType == nil {
		return nil
	}
	return g.mutableSupport(inputType)
}

func (g *ComponentCodegen) mutableInputType() (reflect.Type, error) {
	if g == nil || g.Component == nil {
		return nil, nil
	}
	params := normalizeInputParametersForCodegen(g.Component.InputParameters(), g.Resource, g.Component.URI)
	opts := []state.ReflectOption{state.WithSetMarker(), state.WithTypeName(g.inputTypeName(g.componentName()))}
	if g.componentUsesVelty() {
		opts = append(opts, state.WithVelty(true))
	}
	pkgPath := ""
	if g.TypeContext != nil {
		pkgPath = g.TypeContext.PackagePath
	}
	return params.ReflectType(pkgPath, g.componentLookupType(pkgPath), opts...)
}

func (g *ComponentCodegen) buildMutableVeltyBlock(inputType reflect.Type, support *mutableComponentSupport) (shapeast.Block, error) {
	var block shapeast.Block
	bodyField, ok := inputType.FieldByName(support.BodyFieldName)
	if !ok {
		return nil, nil
	}
	bodyItemType, bodyIsMany := mutableBodyItemType(bodyField.Type)
	if bodyItemType == nil {
		return nil, nil
	}
	bodyKeyField, ok := lookupGeneratedIndexField(bodyItemType)
	if !ok {
		return nil, nil
	}
	bodyTable := g.mutableBodyTableName(support, bodyItemType)
	if bodyTable == "" {
		return nil, nil
	}

	g.appendMutableSequence(&block, shapeast.NewIdent(support.BodyFieldName), "", bodyItemType, bodyTable, bodyKeyField)
	g.appendMutableRelationSequences(&block, shapeast.NewIdent(support.BodyFieldName), "", bodyItemType)

	for _, helper := range support.Helpers {
		block.Append(shapeast.NewAssign(
			shapeast.NewIdent(helper.MapFieldName),
			shapeast.NewCallExpr(shapeast.NewIdent(helper.ViewFieldName), "IndexBy", shapeast.NewQuotedLiteral(helper.KeyFieldName)),
		))
	}
	if len(support.Helpers) > 0 {
		block.AppendEmptyLine()
	}

	rootHelper := support.rootHelper()
	bodyExpr := shapeast.NewIdent(support.BodyFieldName)
	if bodyIsMany {
		recordName := mutableRecordName(support.BodyFieldName)
		forEach := shapeast.NewForEach(shapeast.NewIdent(recordName), bodyExpr, shapeast.Block{})
		g.appendMutableWriteLogic(&forEach.Body, shapeast.NewIdent(recordName), "", bodyItemType, bodyTable, support, rootHelper, bodyKeyField)
		block.Append(forEach)
		return block, nil
	}

	condition := shapeast.NewCondition(bodyExpr, shapeast.Block{}, nil)
	g.appendMutableWriteLogic(&condition.IFBlock, bodyExpr, "", bodyItemType, bodyTable, support, rootHelper, bodyKeyField)
	block.Append(condition)
	return block, nil
}

func (g *ComponentCodegen) appendMutableWriteLogic(block *shapeast.Block, recordExpr *shapeast.Ident, logicalPath string, recordType reflect.Type, tableName string, support *mutableComponentSupport, rootHelper *mutableIndexHelper, keyField reflect.StructField) {
	method := strings.ToUpper(strings.TrimSpace(g.Component.Method))
	hasCurrent := rootHelper != nil
	writeUpdate := method == "PATCH" || method == "PUT"
	writeInsert := method == "PATCH" || method == "POST"
	keyFieldName := keyField.Name

	if hasCurrent && writeUpdate {
		hasKey := shapeast.NewBinary(
			shapeast.NewCallExpr(shapeast.NewIdent(rootHelper.MapFieldName), "HasKey", shapeast.NewIdent(recordExpr.Name+"."+keyFieldName)),
			"==",
			shapeast.NewLiteral("true"),
		)
		condition := shapeast.NewCondition(hasKey, shapeast.Block{}, nil)
		condition.IFBlock.Append(shapeast.NewStatementExpression(shapeast.NewTerminatorExpression(shapeast.NewCallExpr(
			shapeast.NewIdent("sql"), "Update", recordExpr, shapeast.NewQuotedLiteral(tableName),
		))))
		if writeInsert {
			condition.ElseBlock = shapeast.Block{
				shapeast.NewStatementExpression(shapeast.NewTerminatorExpression(shapeast.NewCallExpr(
					shapeast.NewIdent("sql"), "Insert", recordExpr, shapeast.NewQuotedLiteral(tableName),
				))),
			}
		}
		block.Append(condition)
		g.appendChildMutableWriteLogic(block, recordExpr, logicalPath, recordType, support)
		return
	}

	if writeInsert {
		block.Append(shapeast.NewStatementExpression(shapeast.NewTerminatorExpression(shapeast.NewCallExpr(
			shapeast.NewIdent("sql"), "Insert", recordExpr, shapeast.NewQuotedLiteral(tableName),
		))))
	}
	g.appendChildMutableWriteLogic(block, recordExpr, logicalPath, recordType, support)
}

func (g *ComponentCodegen) appendMutableSequence(block *shapeast.Block, bodyExpr *shapeast.Ident, path string, itemType reflect.Type, tableName string, keyField reflect.StructField) {
	if !g.needsMutableSequence(keyField.Type) {
		return
	}
	block.Append(shapeast.NewStatementExpression(shapeast.NewCallExpr(
		shapeast.NewIdent("sequencer"),
		"Allocate",
		shapeast.NewQuotedLiteral(tableName),
		bodyExpr,
		shapeast.NewQuotedLiteral(mutableSequencePath(path, keyField.Name)),
	)))
	block.AppendEmptyLine()
}

func mutableSequencePath(path, key string) string {
	path = strings.Trim(path, "/")
	if path == "" {
		return key
	}
	return path + "/" + key
}

func (g *ComponentCodegen) appendChildMutableWriteLogic(block *shapeast.Block, parentExpr *shapeast.Ident, logicalPath string, parentType reflect.Type, support *mutableComponentSupport) {
	parentType = unwrapNamedStructType(parentType)
	if parentType == nil {
		return
	}
	for i := 0; i < parentType.NumField(); i++ {
		field := parentType.Field(i)
		if !isMutableRelationField(field) {
			continue
		}
		childItemType, childMany := mutableBodyItemType(field.Type)
		if childItemType == nil {
			continue
		}
		childKeyField, ok := lookupGeneratedIndexField(childItemType)
		if !ok {
			continue
		}
		childTable := mutableRelationTableName(field)
		if childTable == "" {
			childTable = tableNameFromType(childItemType.Name())
		}
		if childTable == "" {
			continue
		}
		childPath := mutableSequencePath(logicalPath, field.Name)
		assignments := mutableRelationAssignments(field)
		childHelper := support.findHelper(field.Name, field)

		childExprName := field.Name
		if childMany {
			recordName := mutableRecordName(field.Name)
			forEach := shapeast.NewForEach(shapeast.NewIdent(recordName), shapeast.NewIdent(parentExpr.Name+"."+field.Name), shapeast.Block{})
			appendMutableRelationAssignments(&forEach.Body, shapeast.NewIdent(recordName), parentExpr, assignments, parentType, childItemType)
			g.appendMutableWriteLogic(&forEach.Body, shapeast.NewIdent(recordName), childPath, childItemType, childTable, support, childHelper, childKeyField)
			block.AppendEmptyLine()
			block.Append(forEach)
			continue
		}
		condition := shapeast.NewCondition(shapeast.NewIdent(parentExpr.Name+"."+childExprName), shapeast.Block{}, nil)
		childExpr := shapeast.NewIdent(parentExpr.Name + "." + childExprName)
		appendMutableRelationAssignments(&condition.IFBlock, childExpr, parentExpr, assignments, parentType, childItemType)
		g.appendMutableWriteLogic(&condition.IFBlock, childExpr, childPath, childItemType, childTable, support, childHelper, childKeyField)
		block.AppendEmptyLine()
		block.Append(condition)
	}
}

func (g *ComponentCodegen) appendMutableRelationSequences(block *shapeast.Block, rootExpr *shapeast.Ident, logicalPath string, parentType reflect.Type) {
	parentType = unwrapNamedStructType(parentType)
	if parentType == nil {
		return
	}
	for i := 0; i < parentType.NumField(); i++ {
		field := parentType.Field(i)
		if !isMutableRelationField(field) {
			continue
		}
		childItemType, _ := mutableBodyItemType(field.Type)
		if childItemType == nil {
			continue
		}
		childKeyField, ok := lookupGeneratedIndexField(childItemType)
		if !ok {
			continue
		}
		childTable := mutableRelationTableName(field)
		if childTable == "" {
			childTable = tableNameFromType(childItemType.Name())
		}
		if childTable == "" {
			continue
		}
		childPath := mutableSequencePath(logicalPath, field.Name)
		g.appendMutableSequence(block, rootExpr, childPath, childItemType, childTable, childKeyField)
		g.appendMutableRelationSequences(block, rootExpr, childPath, childItemType)
	}
}

func (g *ComponentCodegen) mutableBodyTableName(support *mutableComponentSupport, bodyItemType reflect.Type) string {
	if support != nil {
		if rootHelper := support.rootHelper(); rootHelper != nil {
			if name := g.mutableTableFromViewState(rootHelper.ViewParamName); name != "" {
				return name
			}
		}
	}
	if g != nil && g.Resource != nil && g.Component != nil {
		rootViewName := strings.TrimSpace(g.Component.RootView)
		if rootViewName != "" {
			if rootView, err := g.Resource.View(rootViewName); err == nil && rootView != nil {
				if rootView.Table != "" {
					return strings.TrimSpace(rootView.Table)
				}
				if rootView.Template != nil {
					if name := tableNameFromSQL(rootView.Template.Source); name != "" {
						return name
					}
				}
				if name := tableNameFromType(rootView.Name); name != "" {
					return name
				}
			}
		}
	}
	if name := tableNameFromType(bodyItemType.Name()); name != "" {
		return name
	}
	return ""
}

func (g *ComponentCodegen) mutableTableFromViewState(viewParamName string) string {
	if g == nil || g.Resource == nil {
		return ""
	}
	for _, input := range g.Component.Input {
		if input == nil || strings.TrimSpace(input.Name) != strings.TrimSpace(viewParamName) {
			continue
		}
		viewName := mutableViewNameFromTag(input.Tag)
		if viewName == "" {
			viewName = strings.TrimSpace(input.Name)
		}
		if viewName == "" {
			continue
		}
		aView, err := g.Resource.View(viewName)
		if err != nil || aView == nil || aView.Template == nil {
			continue
		}
		if table := tableNameFromSQL(aView.Template.Source); table != "" {
			return table
		}
	}
	return ""
}

func mutableViewNameFromTag(tag string) string {
	if tag == "" {
		return ""
	}
	idx := strings.Index(tag, `view:"`)
	if idx == -1 {
		return ""
	}
	rest := tag[idx+len(`view:"`):]
	end := strings.Index(rest, `"`)
	if end == -1 {
		return ""
	}
	return strings.TrimSpace(rest[:end])
}

func tableNameFromSQL(sql string) string {
	fields := strings.Fields(sql)
	for i := 0; i < len(fields)-1; i++ {
		if strings.EqualFold(fields[i], "FROM") {
			candidate := strings.TrimSpace(fields[i+1])
			candidate = strings.Trim(candidate, "`()")
			candidate = strings.TrimRight(candidate, ",;")
			if candidate != "" {
				return candidate
			}
		}
	}
	return ""
}

func tableNameFromType(typeName string) string {
	typeName = strings.TrimSpace(typeName)
	if typeName == "" {
		return ""
	}
	return text.CaseFormatUpperCamel.Format(typeName, text.CaseFormatUpperUnderscore)
}

func mutableBodyItemType(rType reflect.Type) (reflect.Type, bool) {
	for rType != nil && rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	if rType == nil {
		return nil, false
	}
	switch rType.Kind() {
	case reflect.Slice, reflect.Array:
		return unwrapNamedStructType(rType.Elem()), true
	case reflect.Struct:
		return rType, false
	default:
		return nil, false
	}
}

func mutableRecordName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "Rec"
	}
	return "Rec" + name
}

func isMutableRelationField(field reflect.StructField) bool {
	return strings.Contains(field.Tag.Get("view"), "table=") || field.Tag.Get("on") != ""
}

func mutableRelationTableName(field reflect.StructField) string {
	viewTag := field.Tag.Get("view")
	for _, part := range strings.Split(viewTag, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(strings.ToLower(part), "table=") {
			return strings.TrimSpace(strings.TrimPrefix(part, "table="))
		}
	}
	return ""
}

type mutableRelationAssignment struct {
	ParentField string
	ChildField  string
}

func mutableRelationAssignments(field reflect.StructField) []mutableRelationAssignment {
	raw := strings.TrimSpace(field.Tag.Get("on"))
	if raw == "" {
		return nil
	}
	var result []mutableRelationAssignment
	for _, expr := range strings.Split(raw, ",") {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			continue
		}
		parts := strings.Split(expr, "=")
		if len(parts) != 2 {
			continue
		}
		parentField := strings.TrimSpace(strings.Split(strings.TrimSpace(parts[0]), ":")[0])
		childField := strings.TrimSpace(strings.Split(strings.TrimSpace(parts[1]), ":")[0])
		if parentField == "" || childField == "" {
			continue
		}
		result = append(result, mutableRelationAssignment{ParentField: parentField, ChildField: childField})
	}
	return result
}

func appendMutableRelationAssignments(block *shapeast.Block, childExpr, parentExpr *shapeast.Ident, assignments []mutableRelationAssignment, parentType, childType reflect.Type) {
	for _, assignment := range assignments {
		src := shapeast.Expression(shapeast.NewIdent(parentExpr.Name + "." + assignment.ParentField))
		var childFieldType, parentFieldType reflect.Type
		if childType != nil {
			if childField, ok := childType.FieldByName(assignment.ChildField); ok {
				childFieldType = childField.Type
			}
		}
		if parentType != nil {
			if parentField, ok := parentType.FieldByName(assignment.ParentField); ok {
				parentFieldType = parentField.Type
			}
		}
		if childFieldType != nil && parentFieldType != nil {
			childPtr := childFieldType.Kind() == reflect.Ptr
			parentPtr := parentFieldType.Kind() == reflect.Ptr
			if childPtr && !parentPtr {
				src = shapeast.NewRefExpression(src)
			} else if !childPtr && parentPtr {
				src = shapeast.NewDerefExpression(src)
			}
		}
		block.Append(shapeast.NewAssign(shapeast.NewIdent(childExpr.Name+"."+assignment.ChildField), src))
	}
}

func (s *mutableComponentSupport) rootHelper() *mutableIndexHelper {
	if s == nil {
		return nil
	}
	want := "Cur" + s.BodyFieldName
	for i := range s.Helpers {
		if s.Helpers[i].ViewFieldName == want {
			return &s.Helpers[i]
		}
	}
	if len(s.Helpers) == 1 {
		return &s.Helpers[0]
	}
	return nil
}

func (s *mutableComponentSupport) findHelper(fieldName string, field reflect.StructField) *mutableIndexHelper {
	if s == nil {
		return nil
	}
	itemExpr, _ := collectionItemType(field)
	wantSuffix := strings.TrimSpace(fieldName)
	for i := range s.Helpers {
		helper := &s.Helpers[i]
		if itemExpr != "" && strings.EqualFold(strings.TrimSpace(helper.ItemTypeExpr), strings.TrimSpace(itemExpr)) {
			return helper
		}
		if wantSuffix != "" && strings.HasSuffix(strings.TrimSpace(helper.ViewFieldName), wantSuffix) {
			return helper
		}
	}
	return nil
}

func (g *ComponentCodegen) needsMutableSequence(keyType reflect.Type) bool {
	if g == nil || g.Component == nil {
		return false
	}
	method := strings.ToUpper(strings.TrimSpace(g.Component.Method))
	if method != "PATCH" && method != "POST" {
		return false
	}
	for keyType != nil && keyType.Kind() == reflect.Ptr {
		keyType = keyType.Elem()
	}
	if keyType == nil {
		return false
	}
	switch keyType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return false
	}
}

func mutableVeltyOutputUsesBody(aView *view.View) bool {
	return aView != nil
}
