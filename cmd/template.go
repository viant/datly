package cmd

import (
	"context"
	"github.com/viant/afs/file"
	"github.com/viant/datly/config"
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

func (s *Builder) buildTemplate(ctx context.Context, builder *routeBuilder, aViewConfig *ViewConfig, externalParams []*view.Parameter) (*view.Template, error) {
	template, err := s.Parse(ctx, builder, aViewConfig, externalParams)
	if err != nil {
		return nil, err
	}

	parameters, err := s.convertParams(builder, template)
	if err != nil {
		return nil, err
	}

	metaTemplate, err := s.buildTemplateMeta(aViewConfig)
	if err != nil {
		return nil, err
	}

	SQL, URI, err := s.uploadTemplateSQL(builder, template.SQL, aViewConfig)
	if err != nil {
		return nil, err
	}

	return &view.Template{
		Parameters: parameters,
		SourceURL:  URI,
		Meta:       metaTemplate,
		Source:     SQL,
	}, nil
}

func (s *Builder) uploadTemplateSQL(builder *routeBuilder, template string, aViewConfig *ViewConfig) (SQL string, URI string, err error) {
	SQL, err = sanitize.Sanitize(template, builder.paramsIndex.hints, builder.paramsIndex.consts)
	if err != nil {
		return "", "", err
	}

	if SQL != "" && aViewConfig.fileName != "" {
		URI, err = s.upload(
			builder,
			builder.session.TemplateURL(s.fileNames.unique(aViewConfig.fileName)+".sql"),
			SQL,
		)

		if err != nil {
			return "", "", err
		}

		SQL = ""
	}
	return SQL, URI, nil
}

func (s *Builder) Parse(ctx context.Context, builder *routeBuilder, aViewConfig *ViewConfig, params []*view.Parameter) (*Template, error) {
	table := aViewConfig.unexpandedTable

	SQL := table.SQL
	iterator, err := sanitize.NewIterator(SQL, builder.paramsIndex.hints, builder.option.Const, false)
	if err != nil {
		return nil, err
	}
	SQL = iterator.SQL

	defaultParamType := view.KindQuery
	if builder.option.Method != http.MethodGet {
		defaultParamType = view.KindRequestBody
	}

	return NewTemplate(builder.paramsIndex, SQL, defaultParamType, params, s.columnTypes(aViewConfig.expandedTable))
}

func (s *Builder) NewSchema(dataType string, cardinality string) *view.Schema {
	schema := &view.Schema{
		DataType:    dataType,
		Cardinality: view.Cardinality(cardinality),
	}
	return schema
}

func (s *Builder) convertParams(builder *routeBuilder, template *Template) ([]*view.Parameter, error) {
	parameters := template.Parameters
	result := make([]*view.Parameter, 0, len(parameters))
	s.addParameters(builder, template.viewParams...)
	added := map[string]bool{}
	for _, parameter := range parameters {
		existingParam := builder.paramByName(parameter.Name)
		newParam, err := convertMetaParameter(parameter, builder.option.Const, builder.paramsIndex.hints)
		if err != nil {
			return nil, err
		}

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

func convertMetaParameter(param *Parameter, values map[string]interface{}, hints map[string]*sanitize.ParameterHint) (*view.Parameter, error) {
	hint, ok := hints[param.Name]
	if ok {
		jsonHint, _ := sanitize.SplitHint(hint.Hint)
		jsonHint = strings.TrimSpace(jsonHint)
		if err := tryUnmarshalHint(jsonHint, param); err != nil {
			return nil, err
		}
	}

	aCodec, dataType := paramCodec(param)
	constValue := param.Const
	if aValue, ok := values[param.Name]; ok {
		constValue = aValue
	}

	targetName := param.Name
	if param.Location != nil {
		targetName = *param.Location
	}

	cardinality := param.Cardinality
	if param.Multi && param.Kind == string(view.KindDataView) {
		cardinality = view.Many
	}

	return &view.Parameter{
		Name:         param.Id,
		Output:       aCodec,
		Const:        constValue,
		PresenceName: param.Name,
		Schema: &view.Schema{
			DataType:    dataType,
			Cardinality: cardinality,
		},
		In: &view.Location{
			Kind: view.Kind(param.Kind),
			Name: targetName,
		},
		Required: param.Required,
	}, nil
}

func paramCodec(param *Parameter) (*view.Codec, string) {
	dataTypeLower := strings.ToLower(param.DataType)
	if config.CodecKeyAsInts == param.Codec || canInferAsIntsCodec(param, dataTypeLower) {
		return &view.Codec{Reference: shared.Reference{Ref: config.CodecKeyAsInts}}, "string"
	}

	if config.CodecKeyAsStrings == param.Codec || canInferAsStringsCodec(param, dataTypeLower) {
		return &view.Codec{Reference: shared.Reference{Ref: config.CodecKeyAsStrings}}, "string"
	}

	var codec *view.Codec
	if param.Codec != "" {
		codec = &view.Codec{
			Reference: shared.Reference{Ref: param.Codec},
			CodecConfig: config.CodecConfig{
				Query: param.SQL,
			},
		}
	}

	dataType := param.DataType
	if param.Repeated && param.Assumed {
		dataType = "[]" + dataType
	}
	return codec, dataType
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

	if strings.HasPrefix(param.DataType, "interface") || strings.HasPrefix(param.DataType, "[]interface") {
		return false
	}

	return strings.HasPrefix(dataTypeLower, "int") && param.Cardinality == view.One
}

func updateParamPrecedence(dest *view.Parameter, source *view.Parameter) {
	dest.Required = boolPtr(dest.IsRequired() || source.IsRequired())
	if dest.Name == "" {
		dest.Name = source.Name
	}

	if source.DateFormat != "" && dest.DateFormat == "" {
		dest.DateFormat = source.DateFormat
	}

	if dest.In == nil {
		dest.In = source.In
	} else if source.In != nil {
		if source.In.Kind == view.KindDataView {
			dest.In.Kind = source.In.Kind
		}
	}

	if dest.ErrorStatusCode == 0 && source.ErrorStatusCode != 0 {
		dest.ErrorStatusCode = source.ErrorStatusCode
	}

	updateDestSchema(dest, source)
	if dest.In.Kind == view.KindDataView {
		dest.Output = nil
	}

	if source.Const != nil {
		dest.Const = source.Const
	}

	if dest.In != nil && dest.In.Kind == view.KindDataView && dest.Schema != nil {
		dest.Schema.DataType = ""
		dest.Schema.Name = ""
	}

	if dest.In != nil && dest.In.Kind == view.KindParam {
		dest.Schema = nil
	}
}

func updateDestSchema(dest *view.Parameter, source *view.Parameter) {
	if dest.Output != nil {
		return
	}

	if dest.Output == nil {
		dest.Output = source.Output
	}

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

func (s *Builder) buildTemplateMeta(aConfig *ViewConfig) (*view.TemplateMeta, error) {
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
		Kind:   view.MetaKind(shared.FirstNotEmpty(aConfig.outputConfig.Kind, string(view.MetaTypeRecord))),
	}

	return tmplMeta, tryUnmarshalHint(table.ViewHintJSON, tmplMeta)
}

type Template struct {
	SQL        string
	Parameters []*Parameter

	defaultParamKind view.Kind
	variables        map[string]bool
	paramsMeta       *ParametersIndex
	added            map[string]bool
	columnTypes      ColumnIndex
	viewParams       []*view.Parameter
}

func NewTemplate(paramsMeta *ParametersIndex, SQL string, defaultParamKind view.Kind, viewParams []*view.Parameter, columnTypes ColumnIndex) (*Template, error) {
	t := &Template{
		SQL:              SQL,
		paramsMeta:       paramsMeta,
		added:            map[string]bool{},
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

			callExpr := actual.X
			for callExpr != nil {
				switch callType := callExpr.(type) {
				case *expr.Select:
					callExpr = callType.X
				case *expr.Call:
					for _, arg := range callType.Args {
						t.detectParameters([]ast.Statement{arg}, required, arg.Type(), false)
					}
					callExpr = callType.X
				case *expr.SliceIndex:
					t.detectParameters([]ast.Statement{callType.X}, required, callType.Type(), false)
					callExpr = callType.Y
				default:
					callExpr = nil
				}
			}
		case *stmt.Statement:
			selector, ok := asSelector(actual.X)
			if ok {
				t.variables[shared.FirstNotEmpty(selector.FullName, selector.ID)] = true
			}

			t.indexStmt(actual, required, rType, multi)
		case *stmt.ForEach:
			t.variables[actual.Item.ID] = true
			set, ok := actual.Set.(*expr.Select)
			if ok && !t.variables[set.ID] {
				t.detectParameters([]ast.Statement{set}, false, rType, true)
			}

		case *expr.Unary:
			t.detectParameters([]ast.Statement{actual.X}, false, actual.Type(), false)
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

	var prefix, paramName string
	if actual.X != nil {
		if _, ok := actual.X.(*expr.Call); ok {
			paramName = actual.ID
		}
	}

	if paramName == "" {
		prefix, paramName = sanitize.GetHolderName(actual.FullName)
	}

	if !isParameter(t.variables, paramName) {
		return
	}

	selector, ok := getContextSelector(prefix, actual.X)
	if ok {
		multi = multi || selector.ID == "IndexBy"
	}

	param := t.ParamByName(paramName)

	pType := "string"
	assumed := param.Assumed
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

	if param.Id == "" {
		param.Id = paramName
	}

	if param.Name == "" {
		param.Name = paramName
	}

	if param.FullName == "" {
		param.FullName = actual.FullName
	}

	if param.Kind == "" {
		param.Kind = kind
	}

	if param.Assumed {
		if param.DataType == "string" || param.DataType == "" {
			param.DataType = pType
		}
	}

	param.Assumed = param.Assumed && assumed
	param.Multi = param.Multi || multi
	param.Has = param.Has || prefix == keywords.ParamsMetadataKey
	param.Required = BoolPtr((required && prefix != keywords.ParamsMetadataKey) || param.Required != nil && *param.Required)
	t.AddParameter(param)
}

func getContextSelector(prefix string, x ast.Expression) (*expr.Select, bool) {
	selector, ok := asSelector(x)
	if prefix == "" || !ok {
		return selector, ok
	}

	return asSelector(selector.X)
}

func asSelector(x ast.Expression) (*expr.Select, bool) {
	selector, ok := x.(*expr.Select)
	return selector, ok
}

func (t *Template) AddParameter(param *Parameter) {
	if t.variables != nil && t.variables[param.Name] || !sanitize.CanBeParam(param.Name) {
		return
	}

	if !t.added[param.Id] {
		t.Parameters = append(t.Parameters, param)
		t.added[param.Id] = true
	}
}

func (t *Template) unmarshalParamsHints() error {
	iterator, err := sanitize.NewIterator(t.SQL, t.paramsMeta.hints, t.paramsMeta.consts, false)
	if err != nil {
		return err
	}

	for iterator.Has() {
		paramMeta := iterator.Next()
		aParam := t.ParamByName(paramMeta.Holder)
		if err := t.updateParamIfNeeded(aParam, paramMeta); err != nil {
			return err
		}
	}

	return nil
}

func (t *Template) updateParamIfNeeded(param *Parameter, meta *sanitize.ParamMeta) error {
	if value, ok := t.paramsMeta.consts[param.Name]; ok {
		param.Kind = string(view.KindLiteral)
		param.DataType = reflect.TypeOf(value).String()
		param.Const = value
	}

	if meta.MetaType == nil {
		return nil
	}

	oldType := param.DataType
	_, err := sanitize.UnmarshalHint(meta.MetaType.Hint, param)
	if err != nil {
		return err
	}
	param.Assumed = param.Assumed && oldType == param.DataType
	param.Typer = meta.MetaType.Typer

	if strings.EqualFold(meta.SQLKeyword, sanitize.InKeyword) || strings.Contains(meta.FnName, "criteria.In") {
		param.Repeated = true
	}

	return nil
}

func (t *Template) ParamByName(holder string) *Parameter {
	return t.paramsMeta.ParamMeta(holder)
}

func (t *Template) inheritParamTypesFromTypers() error {
	for _, p := range t.Parameters {
		if !p.Assumed {
			continue
		}

		var dataType string

		for _, typer := range p.Typer {
			var currType string
			switch actual := typer.(type) {
			case *sanitize.ColumnType:
				meta := t.columnTypes[strings.ToLower(actual.ColumnName)]
				if meta != nil {
					currType = meta.Type.String()
				}
			case *sanitize.LiteralType:
				currType = actual.RType.String()
			}

			if dataType == "" {
				dataType = currType
				continue
			}

			if currType == "" {
				continue
			}

			if currType != "string" && dataType == "string" {
				dataType = currType
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

func (s *Builder) upload(builder *routeBuilder, destURL string, fileContent string) (string, error) {
	if err := s.fs.Upload(context.Background(), destURL, file.DefaultFileOsMode, strings.NewReader(fileContent)); err != nil {
		return "", err
	}

	return builder.session.RelativeOfBasePath(destURL), nil
}

func (s *Builder) newViewField(column *Column, columnTypes map[string]*ColumnMeta, structFieldName string) *view.Field {
	dataType := column.DataType
	if dataType == "" || dataType == "string" {
		meta, ok := columnTypes[strings.ToLower(structFieldName)]
		if ok {
			dataType = meta.Type.String()
		}
	}

	if dataType == "" {
		dataType = "string"
	}

	aField := &view.Field{
		Name:   structFieldName,
		Schema: &view.Schema{DataType: dataType},
	}
	return aField
}

func BoolPtr(b bool) *bool {
	return &b
}
