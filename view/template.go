package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/datly/view/parameter"
	rdata "github.com/viant/toolbox/data"
	"github.com/viant/velty"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"sort"
	"strings"
)

var boolType = reflect.TypeOf(true)

type (
	Template struct {
		Source         string        `json:",omitempty" yaml:"source,omitempty"`
		SourceURL      string        `json:",omitempty" yaml:"sourceURL,omitempty"`
		Schema         *Schema       `json:",omitempty" yaml:"schema,omitempty"`
		PresenceSchema *Schema       `json:",omitempty" yaml:"presenceSchema,omitempty"`
		Parameters     []*Parameter  `json:",omitempty" yaml:"parameters,omitempty"`
		Meta           *TemplateMeta `json:",omitempty" yaml:",omitempty"`

		sqlEvaluator     *expand.Evaluator
		accessors        *types.Accessors
		_fields          []reflect.StructField
		_fieldIndex      map[string]int
		_parametersIndex ParametersIndex
		initialized      bool
		isTemplate       bool
		wasEmpty         bool
		_view            *View
	}

	TemplateOption func(t *Template)

	CriteriaParam struct {
		ColumnsIn   string `velty:"COLUMN_IN"`
		WhereClause string `velty:"CRITERIA"`
		Pagination  string `velty:"PAGINATION"`
		Qualifier   ParamQualifier
	}

	ParamQualifier struct {
		SQL  string
		Args []interface{}
	}
)

func (t *Template) Init(ctx context.Context, resource *Resource, view *View) error {
	if t.initialized {
		return nil
	}

	t.wasEmpty = t.Source == "" && t.SourceURL == ""
	t.initialized = true

	err := t.loadSourceFromURL(ctx, resource)
	if err != nil {
		return err
	}

	t._view = view
	t._fieldIndex = map[string]int{}
	t.updateSource(view)

	t.isTemplate = t.Source != view.Name && t.Source != view.Table

	if err = t.initTypes(ctx, resource); err != nil {
		return err
	}

	if err = t.initPresenceType(resource); err != nil {
		return err
	}

	if err = t.initSqlEvaluator(resource); err != nil {
		return err
	}

	t.initAccessors()

	if err = t.updateParametersFields(); err != nil {
		return err
	}

	t._parametersIndex, err = ParametersSlice(t.Parameters).Index()
	if err != nil {
		return err
	}

	sort.Sort(ParametersSlice(t.Parameters))
	return t.initMetaIfNeeded(ctx, resource)
}

func (t *Template) updateSource(view *View) {
	if t.Source != "" {
		t.Source = "( " + t.Source + " )"
		return
	}

	t.Source = view.Source()
}

func (t *Template) loadSourceFromURL(ctx context.Context, resource *Resource) error {
	if t.SourceURL == "" {
		return nil
	}
	var err error
	t.Source, err = resource.LoadText(ctx, t.SourceURL)
	return err
}

func (t *Template) initTypes(ctx context.Context, resource *Resource) error {
	if t.Schema == nil || (t.Schema.Name == "" && t.Schema.Type() == nil) {
		return t.createSchemaFromParams(ctx, resource)
	}

	return t.inheritParamTypesFromSchema(ctx, resource)
}

func (t *Template) createSchemaFromParams(ctx context.Context, resource *Resource) error {
	for _, param := range t.Parameters {
		if err := t.inheritAndInitParam(ctx, resource, param); err != nil {
			return err
		}
	}

	rType, err := BuildType(t.Parameters)
	if err != nil {
		return err
	}

	t.Schema = &Schema{}
	t.Schema.SetType(reflect.PtrTo(rType))
	return nil
}

func BuildType(parameters []*Parameter) (reflect.Type, error) {
	builder := parameter.NewBuilder("")
	for _, param := range parameters {
		if err := builder.AddType(param.Name, param.ActualParamType()); err != nil {
			return nil, err
		}
	}

	return builder.Build(), nil
}

func BuildPresenceType(parameters []*Parameter) (reflect.Type, error) {
	builder := parameter.NewBuilder("")
	for _, param := range parameters {
		if err := builder.AddType(param.PresenceName, boolType); err != nil {
			return nil, err
		}
	}

	return builder.Build(), nil
}

func (t *Template) addField(name string, rType reflect.Type) error {
	_, ok := t._fieldIndex[name]
	if ok {
		return fmt.Errorf("_field with %v name already exists", name)
	}

	field, err := TemplateField(name, rType)
	if err != nil {
		return err
	}

	t._fieldIndex[name] = len(t._fields)
	t._fields = append(t._fields, field)

	return nil
}

func TemplateField(name string, rType reflect.Type) (reflect.StructField, error) {
	if len(name) == 0 {
		return reflect.StructField{}, fmt.Errorf("template field name can't be empty")
	}

	pkgPath := ""
	if name[0] < 'A' || name[0] > 'Z' {
		pkgPath = "github.com/viant/datly/router"
	}

	field := reflect.StructField{Name: name, Type: rType, PkgPath: pkgPath}
	return field, nil
}

func (t *Template) inheritParamTypesFromSchema(ctx context.Context, resource *Resource) error {
	if t.Schema.Type() == nil {
		rType, err := resource._types.Lookup(t.Schema.Name)
		if err != nil {
			return err
		}
		t.Schema.SetType(rType)
	}

	for _, param := range t.Parameters {
		if err := t.inheritAndInitParam(ctx, resource, param); err != nil {
			return err
		}

		if err := param.Init(ctx, t._view, resource, t.Schema.Type()); err != nil {
			return err
		}
	}

	return nil
}

func NewEvaluator(parameters []*Parameter, paramSchema, presenceSchema reflect.Type, template string, typeLookup xreflect.TypeLookupFn) (*expand.Evaluator, error) {
	return expand.NewEvaluator(FilterConstParameters(parameters), paramSchema, presenceSchema, template, typeLookup)
}

func FilterConstParameters(parameters []*Parameter) []expand.ConstUpdater {
	params := make([]expand.ConstUpdater, 0)
	for i := range parameters {
		if parameters[i].In.Kind != KindLiteral {
			continue
		}

		params = append(params, parameters[i])
	}

	return params
}

func (t *Template) EvaluateSource(externalParams, presenceMap interface{}, parentParam *expand.MetaParam, batchData *BatchData, options ...interface{}) (*expand.State, error) {
	if t.wasEmpty {
		return expand.StateWithSQL(t.Source), nil
	}

	return t.EvaluateState(externalParams, presenceMap, parentParam, batchData, options...)
}

func (t *Template) EvaluateState(externalParams interface{}, presenceMap interface{}, parentParam *expand.MetaParam, batchData *BatchData, options ...interface{}) (*expand.State, error) {
	var expander expand.Expander
	for _, option := range options {
		switch actual := option.(type) {
		case expand.Expander:
			expander = actual
		}
	}

	return Evaluate(t.sqlEvaluator, externalParams, presenceMap, AsViewParam(t._view, nil, batchData, expander), parentParam)
}

//WithTemplateParameter return parameter template options
func WithTemplateParameter(parameter *Parameter) TemplateOption {
	return func(t *Template) {
		t.Parameters = append(t.Parameters, parameter)
	}
}

//NewTemplate creates a template
func NewTemplate(source string, opts ...TemplateOption) *Template {
	ret := &Template{Source: source}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func Evaluate(evaluator *expand.Evaluator, externalParams, presenceMap interface{}, viewParam, parentParam *expand.MetaParam) (*expand.State, error) {
	return evaluator.Evaluate(externalParams, presenceMap, viewParam, parentParam, nil)
}

func AsViewParam(aView *View, aSelector *Selector, batchData *BatchData, options ...interface{}) *expand.MetaParam {
	var metaSource expand.MetaSource
	if aView != nil {
		metaSource = aView
	}

	var metaExtras expand.MetaExtras
	if aSelector != nil {
		metaExtras = aSelector
	}

	var metaBatch expand.MetaBatch
	if batchData != nil {
		metaBatch = batchData
	}

	return expand.NewMetaParam(metaSource, metaExtras, metaBatch, options...)
}

func (t *Template) inheritAndInitParam(ctx context.Context, resource *Resource, param *Parameter) error {
	return param.Init(ctx, t._view, resource, nil)
}

func (t *Template) initSqlEvaluator(resource *Resource) error {
	if t.wasEmpty {
		return nil
	}

	evaluator, err := NewEvaluator(t.Parameters, t.Schema.Type(), t.PresenceSchema.Type(), t.Source, resource.LookupType)
	if err != nil {
		return err
	}

	t.sqlEvaluator = evaluator
	return nil
}

func (t *Template) initPresenceType(resource *Resource) error {
	if t.PresenceSchema == nil {
		return t.initPresenceSchemaFromParams()
	}

	if t.PresenceSchema.Type() != nil {
		return nil
	}

	rType, err := resource._types.Lookup(t.PresenceSchema.Name)
	if err != nil {
		return err
	}

	t.PresenceSchema.SetType(rType)
	return nil
}

func (t *Template) initPresenceSchemaFromParams() error {
	rType, err := BuildPresenceType(t.Parameters)
	if err != nil {
		return err
	}

	t.PresenceSchema = &Schema{}
	t.PresenceSchema.SetType(rType)

	return nil
}

func (t *Template) updateParametersFields() error {
	for _, param := range t.Parameters {
		if err := param.SetPresenceField(t.PresenceSchema.Type()); err != nil {
			return err
		}

		accessor, err := t.AccessorByName(param.Name)
		if err != nil {
			return err
		}

		param.SetAccessor(accessor)
	}

	return nil
}

func (t *Template) initAccessors() {
	if t.accessors == nil {
		t.accessors = types.NewAccessors(&types.VeltyNamer{})
	}

	t.accessors.Init(t.Schema.Type())
}

func (t *Template) AccessorByName(name string) (*types.Accessor, error) {
	return t.accessors.AccessorByName(name)
}

func fieldByTemplateName(structType reflect.Type, name string) (*xunsafe.Field, error) {
	structType = shared.Elem(structType)

	field, ok := structType.FieldByName(name)
	if !ok {
		for i := 0; i < structType.NumField(); i++ {
			field = structType.Field(i)
			veltyTag := velty.Parse(field.Tag.Get("velty"))
			for _, fieldName := range veltyTag.Names {
				if fieldName == name {
					return xunsafe.NewField(field), nil
				}
			}
		}

		return nil, fmt.Errorf("not found _field %v at type %v", name, structType.String())
	}

	return xunsafe.NewField(field), nil
}

func (t *Template) IsActualTemplate() bool {
	return t.isTemplate
}

func (t *Template) Expand(placeholders *[]interface{}, SQL string, selector *Selector, params CriteriaParam, batchData *BatchData, sanitized *expand.DataUnit) (string, error) {
	values, err := parameter.Parse(SQL)
	if err != nil {
		return "", err
	}

	replacement := &rdata.Map{}

	for _, value := range values {
		fmt.Printf("expanding values: %v %v\n", value.Key, value.TemplateFragment)
		if value.Key == "?" {
			placeholder, err := sanitized.Next()
			if err != nil {
				return "", err
			}

			*placeholders = append(*placeholders, placeholder)
			continue
		}

		key, val, err := t.prepareExpanded(value, params, selector, batchData, placeholders, sanitized)
		if err != nil {
			return "", err
		}

		if key == "" {
			continue
		}

		replacement.SetValue(key, val)
	}

	return replacement.ExpandAsText(SQL), err
}

func (t *Template) prepareExpanded(value *parameter.Value, params CriteriaParam, selector *Selector, batchData *BatchData, placeholders *[]interface{}, sanitized *expand.DataUnit) (string, string, error) {
	key, val, err := t.replacementEntry(value.Key, params, selector, batchData, placeholders, sanitized)
	if err != nil {
		return "", "", err
	}

	return key, val, err
}

func (t *Template) replacementEntry(key string, params CriteriaParam, selector *Selector, batchData *BatchData, placeholders *[]interface{}, sanitized *expand.DataUnit) (string, string, error) {
	switch key {
	case keywords.Pagination[1:]:
		return key, params.Pagination, nil
	case keywords.Criteria[1:]:
		criteriaExpanded, err := t.Expand(placeholders, params.WhereClause, selector, params, batchData, sanitized)
		if err != nil {
			return "", "", err
		}

		return key, criteriaExpanded, nil
	case keywords.ColumnsIn[1:]:
		*placeholders = append(*placeholders, batchData.ValuesBatch...)
		return key, params.ColumnsIn, nil
	case keywords.SelectorCriteria[1:]:
		*placeholders = append(*placeholders, selector.Placeholders...)
		criteria := selector.Criteria

		if len(params.Qualifier.SQL) > 0 {
			*placeholders = append(*placeholders, params.Qualifier.Args...)
			if len(criteria) > 0 {
				criteria = criteria + " AND "
			}

			criteria = criteria + params.Qualifier.SQL
		}

		return key, criteria, nil
	default:
		if strings.HasPrefix(key, keywords.WherePrefix) {
			_, aValue, err := t.replacementEntry(key[len(keywords.WherePrefix):], params, selector, batchData, placeholders, sanitized)
			if err != nil {
				return "", "", err
			}

			return t.valueWithPrefix(key, aValue, " WHERE ", false)
		}

		if strings.HasPrefix(key, keywords.AndPrefix) {
			_, aValue, err := t.replacementEntry(key[len(keywords.AndPrefix):], params, selector, batchData, placeholders, sanitized)
			if err != nil {
				return "", "", err
			}

			return t.valueWithPrefix(key, aValue, " AND ", true)
		}

		if strings.HasPrefix(key, keywords.OrPrefix) {
			_, aValue, err := t.replacementEntry(key[len(keywords.OrPrefix):], params, selector, batchData, placeholders, sanitized)
			if err != nil {
				return "", "", err
			}

			return t.valueWithPrefix(key, aValue, " OR ", true)
		}

		accessor, err := t.AccessorByName(key)
		if err != nil {
			return "", "", err
		}

		values, err := accessor.Values(selector.Parameters.Values)
		if err != nil {
			return "", "", err
		}

		*placeholders = append(*placeholders, values...)
		actualKey, bindings := expand.AsBindings(key, values)
		return actualKey, bindings, nil
	}
}

func (t *Template) valueWithPrefix(key string, aValue, prefix string, wrapWithParentheses bool) (string, string, error) {
	if aValue == "" {
		return key, "", nil
	}

	if wrapWithParentheses {
		return key, prefix + "(" + aValue + ")", nil
	}

	return key, prefix + aValue, nil
}

func (t *Template) initMetaIfNeeded(ctx context.Context, r *Resource) error {
	if t.Meta == nil {
		return nil
	}

	return t.Meta.Init(ctx, t, r)
}

func (t *Template) StateType() reflect.Type {
	return t.sqlEvaluator.Type()
}
