package cmd

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/datly/gateway/registry"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/template/sanitize"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	"github.com/viant/velty/parser"
	"net/http"
	"reflect"
	"strings"
)

func (s *Builder) buildTemplate(ctx context.Context, aViewConfig *viewConfig, externalParams []*view.Parameter) (*view.Template, error) {
	template, err := s.Parse(ctx, aViewConfig, externalParams)
	if err != nil {
		return nil, err
	}

	parameters, err := s.convertParams(template)
	if err != nil {
		return nil, err
	}

	metaTemplate, err := s.buildTemplateMeta(aViewConfig)
	if err != nil {
		return nil, err
	}

	SQL := sanitize.Sanitize(template.SQL, s.routeBuilder.paramsIndex.hints, s.routeBuilder.paramsIndex.consts)
	var URI string
	if SQL != "" && aViewConfig.fileName != "" {
		URI, err = s.uploadSQL(aViewConfig.fileName, SQL)
		if err != nil {
			return nil, err
		}

		SQL = ""
	}

	return &view.Template{
		Parameters: parameters,
		SourceURL:  URI,
		Meta:       metaTemplate,
		Source:     SQL,
	}, nil
}

func (s *Builder) Parse(ctx context.Context, aViewConfig *viewConfig, params []*view.Parameter) (*Template, error) {
	table := aViewConfig.unexpandedTable

	SQL := table.SQL
	iterator := sanitize.NewIterator(SQL, s.routeBuilder.paramsIndex.hints, s.routeBuilder.option.Const)
	SQL = iterator.SQL

	defaultParamType := view.QueryKind
	if s.routeBuilder.option.Method == http.MethodPost {
		defaultParamType = view.RequestBodyKind
	}

	return NewTemplate(s.routeBuilder.paramsIndex, SQL, defaultParamType, params, s.columnTypes(aViewConfig.expandedTable))
}

func (s *Builder) NewSchema(dataType string, cardinality string) *view.Schema {
	schema := &view.Schema{
		DataType:    dataType,
		Cardinality: view.Cardinality(cardinality),
	}
	return schema
}

func (s *Builder) convertParams(template *Template) ([]*view.Parameter, error) {
	parameters := template.Parameters
	result := make([]*view.Parameter, 0, len(parameters))
	s.addParameters(template.viewParams...)

	added := map[string]bool{}
	for _, parameter := range parameters {
		existingParam := s.paramByName(parameter.Name)
		newParam := convertMetaParameter(parameter, s.routeBuilder.option.Const)
		updateParamPrecedence(existingParam, newParam)
		result = append(result, &view.Parameter{Reference: shared.Reference{Ref: existingParam.Name}})
		added[existingParam.Name] = true
	}

	for _, param := range template.viewParams {
		if added[param.Name] {
			continue
		}

		result = append(result, &view.Parameter{Reference: shared.Reference{Ref: param.Name}})
	}

	return result, nil
}

func convertMetaParameter(param *Parameter, values map[string]interface{}) *view.Parameter {
	aCodec, dataType := paramCodec(param)
	constValue := param.Const
	if aValue, ok := values[param.Name]; ok {
		constValue = aValue
	}

	return &view.Parameter{
		Name:         param.Id,
		Codec:        aCodec,
		Const:        constValue,
		PresenceName: param.Name,
		Schema: &view.Schema{
			DataType:    dataType,
			Cardinality: param.Cardinality,
		},
		In: &view.Location{
			Kind: view.Kind(param.Kind),
			Name: param.Name,
		},
		Required: param.Required,
	}
}

func paramCodec(param *Parameter) (*view.Codec, string) {
	dataTypeLower := strings.ToLower(param.DataType)
	if registry.CodecKeyAsInts == param.Codec || canInferAsIntsCodec(param, dataTypeLower) {
		return &view.Codec{Reference: shared.Reference{Ref: registry.CodecKeyAsInts}}, "string"
	}

	if registry.CodecKeyAsStrings == param.Codec || canInferAsStringsCodec(param, dataTypeLower) {
		return &view.Codec{Reference: shared.Reference{Ref: registry.CodecKeyAsStrings}}, "string"

	}

	return nil, param.DataType
}

func canInferAsStringsCodec(param *Parameter, dataTypeLower string) bool {
	if !param.Repeated || param.Codec != "" || !param.Assumed {
		return false
	}

	return strings.HasPrefix(dataTypeLower, "[]string")
}

func canInferAsIntsCodec(param *Parameter, dataTypeLower string) bool {
	if !param.Repeated || param.Codec != "" || !param.Assumed {
		return false
	}

	if strings.HasPrefix(param.DataType, "interface") {
		return false
	}

	return strings.HasPrefix(dataTypeLower, "[]int")
}

func updateParamPrecedence(dest *view.Parameter, source *view.Parameter) {
	dest.Required = boolPtr(dest.IsRequired() || source.IsRequired())
	if dest.Name == "" {
		dest.Name = source.Name
	}

	if source.DateFormat != "" && dest.DateFormat == "" {
		dest.DateFormat = source.DateFormat
	}

	if dest.Codec == nil {
		dest.Codec = source.Codec
	}

	if dest.In == nil {
		dest.In = source.In
	} else if source.In != nil {
		if source.In.Kind == view.DataViewKind {
			dest.In.Kind = source.In.Kind
		}
	}

	if dest.ErrorStatusCode == 0 && source.ErrorStatusCode != 0 {
		dest.ErrorStatusCode = source.ErrorStatusCode
	}

	updateDestSchema(dest, source)
	if dest.In.Kind == view.DataViewKind {
		dest.Codec = nil
	}

	if source.Const != nil {
		dest.Const = source.Const
	}
}

func updateDestSchema(dest *view.Parameter, source *view.Parameter) {
	if source.Schema == nil {
		return
	}

	if dest.Schema == nil {
		dest.Schema = source.Schema
		return
	}

	if dest.Schema.Cardinality != view.Many {
		dest.Schema.Cardinality = source.Schema.Cardinality
	}

	if dest.Schema.DataType == "" {
		dest.Schema.DataType = source.Schema.DataType
	}
}

func (s *Builder) buildTemplateMeta(aConfig *viewConfig) (*view.TemplateMeta, error) {
	var table *Table
	if aConfig.templateMeta != nil {
		table = aConfig.templateMeta.table
	}

	if table == nil {
		return nil, nil
	}

	viewAlias := getMetaTemplateHolder(table.Name)
	SQL := normalizeMetaTemplateSQL(table.SQL, viewAlias)
	tmplMeta := &view.TemplateMeta{
		Source: SQL,
		Name:   table.HolderName,
		Kind:   view.MetaKind(view.NotEmptyOf(aConfig.outputConfig.Kind, string(view.MetaTypeRecord))),
	}

	return tmplMeta, tryUnmarshalHint(table.ViewHintJSON, tmplMeta)
}

type Template struct {
	SQL        string
	Parameters []*Parameter

	defaultParamKind view.Kind
	variables        map[string]bool
	paramsMeta       *ParametersIndex
	index            map[string]int
	columnTypes      ColumnIndex
	viewParams       []*view.Parameter
}

func NewTemplate(paramsMeta *ParametersIndex, SQL string, defaultParamKind view.Kind, viewParams []*view.Parameter, columnTypes ColumnIndex) (*Template, error) {
	t := &Template{
		SQL:              SQL,
		paramsMeta:       paramsMeta,
		index:            map[string]int{},
		defaultParamKind: defaultParamKind,
		columnTypes:      columnTypes,
		viewParams:       viewParams,
		variables:        map[string]bool{},
	}

	return t, t.Init()
}

func (t *Template) Init() error {
	if err := t.tryDetectParameters(); err != nil {
		return err
	}

	if err := t.unmarshalParamsHints(); err != nil {
		return err
	}

	if err := t.inheritParamTypesFromTypers(); err != nil {
		return err
	}

	return nil
}

func (t *Template) tryDetectParameters() error {
	aBlock, err := parser.Parse([]byte(t.SQL))
	if err != nil {
		return err
	}

	t.detectParameters(aBlock.Stmt, true, nil, false)
	return nil
}

func (t *Template) detectParameters(statements []ast.Statement, required bool, rType reflect.Type, multi bool) {
	for _, statement := range statements {
		switch actual := statement.(type) {
		case stmt.ForEach:
			t.variables[actual.Item.ID] = true
		case stmt.Statement:
			t.indexStmt(&actual, required, rType, multi)
		case *expr.Select:
			t.indexParameter(actual, required, rType, multi)
		case *stmt.Statement:
			t.indexStmt(actual, required, rType, multi)
		case *stmt.ForEach:
			t.variables[actual.Item.ID] = true
			set, ok := actual.Set.(*expr.Select)
			if ok && !t.variables[set.ID] {
				t.detectParameters([]ast.Statement{set}, false, rType, true)
			}

		case *expr.Unary:
			t.detectParameters([]ast.Statement{actual}, false, actual.Type(), false)
		case *expr.Binary:
			xType := actual.X.Type()
			if xType == nil {
				xType = actual.Y.Type()
			}

			t.detectParameters([]ast.Statement{actual.X, actual.Y}, false, xType, false)
		case *expr.Parentheses:
			t.detectParameters([]ast.Statement{actual.P}, false, actual.Type(), false)
		case *stmt.If:
			t.detectParameters([]ast.Statement{actual.Condition}, false, actual.Type(), false)
			if actual.Else != nil {
				t.detectParameters([]ast.Statement{actual.Else}, false, actual.Else.Type(), false)
			}
		}

		switch actual := statement.(type) {
		case ast.StatementContainer:
			t.detectParameters(actual.Statements(), false, nil, false)
		}
	}
}

func (t *Template) indexStmt(actual *stmt.Statement, required bool, rType reflect.Type, multi bool) {
	x, ok := actual.X.(*expr.Select)
	if ok {
		t.variables[x.ID] = true
	}

	y, ok := actual.Y.(*expr.Select)
	if ok && !t.variables[y.ID] {
		t.indexParameter(y, required, rType, multi)
	}
}

func (t *Template) indexParameter(actual *expr.Select, required bool, rType reflect.Type, multi bool) {
	prefix, paramName := sanitize.GetHolderName(actual.FullName)

	if !isParameter(t.variables, paramName) {
		return
	}

	pType := "string"
	assumed := true

	if declared, ok := t.paramsMeta.types[paramName]; ok {
		pType = declared
		assumed = false
	}

	if rType != nil && prefix != keywords.ParamsMetadataKey {
		pType = rType.String()
		assumed = false
	}

	kind := string(t.defaultParamKind)
	if paramKind, ok := t.paramsMeta.ParamType(paramName); ok {
		kind = string(paramKind)
	}

	t.AddParameter(&Parameter{
		Assumed:  assumed,
		Id:       paramName,
		Name:     paramName,
		Kind:     kind,
		DataType: pType,
		FullName: actual.FullName,
		Multi:    multi,
		Required: BoolPtr(required && prefix != keywords.ParamsMetadataKey),
		Has:      prefix == keywords.ParamsMetadataKey,
	})
}

func (t *Template) AddParameter(param *Parameter) {
	if t.variables != nil && t.variables[param.Name] || !sanitize.CanBeParam(param.Name) {
		return
	}

	if param.Multi {
		param.Cardinality = view.Many
	}

	if index, ok := t.index[param.Id]; ok {
		parameter := t.Parameters[index]
		parameter.Multi = param.Multi || parameter.Multi
		if parameter.Multi {
			parameter.Cardinality = view.Many
		}

		parameter.Repeated = parameter.Repeated || param.Repeated

		parameter.Required = BoolPtr(((parameter.Required != nil && *parameter.Required) || (param.Required != nil && *param.Required)) && !(param.Has || parameter.Has))
		if parameter.Assumed {
			parameter.DataType = param.DataType
		}

		return
	}

	t.index[param.Id] = len(t.Parameters)
	t.Parameters = append(t.Parameters, param)
}

func (t *Template) unmarshalParamsHints() error {
	iterator := sanitize.NewIterator(t.SQL, t.paramsMeta.hints, t.paramsMeta.consts)
	for iterator.Has() {
		paramMeta := iterator.Next()
		aParam, ok := t.ParamByName(paramMeta.Holder)
		if !ok {
			continue
		}

		if err := t.updateParamIfNeeded(aParam, paramMeta); err != nil {
			return err
		}
	}

	return nil
}

func (t *Template) updateParamIfNeeded(param *Parameter, meta *sanitize.ParamMeta) error {
	if value, ok := t.paramsMeta.consts[param.Name]; ok {
		param.Kind = string(view.LiteralKind)
		param.DataType = reflect.TypeOf(value).String()
		param.Const = value
	}

	if meta.MetaType == nil {
		return nil
	}

	for _, aHint := range meta.MetaType.Hint {
		oldType := param.DataType
		_, err := sanitize.UnmarshalHint(aHint, param)
		if err != nil {
			return err
		}

		param.Assumed = !param.Assumed && oldType == param.DataType
	}

	if len(meta.MetaType.SQL) > 1 {
		return fmt.Errorf("found multiple SQL statements for one parameter %v, SQL: %v", param.Name, meta.MetaType.SQL)
	}

	if len(meta.MetaType.SQL) == 1 {
		param.Kind = string(view.DataViewKind)
		param.SQL = meta.MetaType.SQL[0]
	}

	if len(meta.MetaType.Typer) > 0 {
		param.Typer = meta.MetaType.Typer[0]
	}

	if strings.EqualFold(meta.SQLKeyword, sanitize.InKeyword) {
		param.Repeated = true
	}

	return nil
}

func (t *Template) ParamByName(holder string) (*Parameter, bool) {
	index, ok := t.index[holder]
	if !ok {
		return nil, false
	}

	return t.Parameters[index], true
}

func (t *Template) inheritParamTypesFromTypers() error {
	for _, p := range t.Parameters {
		if !p.Assumed {
			continue
		}

		var dataType string
		if p.Typer != nil {
			switch actual := p.Typer.(type) {
			case *sanitize.ColumnType:
				meta := t.columnTypes[strings.ToLower(actual.ColumnName)]
				if meta != nil {
					dataType = meta.Type.String()
				}
			case *sanitize.LiteralType:
				dataType = actual.RType.String()
			}
		}

		if dataType == "" {
			meta := t.columnTypes[strings.ToLower(p.Name)]
			if meta != nil {
				dataType = meta.Type.String()
			}
		}

		if dataType == "" {
			dataType = "string"
		}

		if p.Repeated {
			dataType = "[]" + dataType
		}

		p.DataType = dataType
	}

	return nil
}

func isParameter(variables map[string]bool, paramName string) bool {
	if isVariable := variables[paramName]; isVariable {
		return false
	}

	return sanitize.CanBeParam(paramName)
}

func (s *Builder) uploadSQL(fileName string, SQL string) (string, error) {
	sourceURL := s.options.SQLURL(fileName, true)
	fs := afs.New()
	if err := fs.Upload(context.Background(), sourceURL, file.DefaultFileOsMode, strings.NewReader(SQL)); err != nil {
		return "", err
	}

	skipped := 0
	anIndex := strings.LastIndexFunc(sourceURL, func(r rune) bool {
		if r == '/' {
			skipped++
		}

		if skipped == 2 {
			return true
		}
		return false
	})
	sourceURL = sourceURL[anIndex+1:]
	return sourceURL, nil
}

func (s *Builder) buildSchemaFromTable(schemaName string, table *Table, columnTypes map[string]*ColumnMeta) *view.Definition {
	var fields = make([]*view.Field, 0)
	for _, column := range table.Inner {
		structFieldName := column.Alias
		if structFieldName == "" {
			structFieldName = column.Name
		}

		if structFieldName == "" {
			continue
		}

		dataType := column.DataType
		if dataType == "" {
			meta, ok := columnTypes[strings.ToLower(structFieldName)]
			if ok {
				dataType = meta.Type.String()
			}
		}

		if dataType == "" {
			dataType = "string"
		}

		fields = append(fields, &view.Field{
			Name:   structFieldName,
			Embed:  false,
			Schema: &view.Schema{DataType: dataType},
		})
	}

	return &view.Definition{
		Name:   schemaName,
		Fields: fields,
		Schema: nil,
	}
}

func BoolPtr(b bool) *bool {
	return &b
}
