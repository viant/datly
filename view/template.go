package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/executor/session"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/datly/view/parameter"
	"github.com/viant/structology"
	rdata "github.com/viant/toolbox/data"
	"github.com/viant/velty"
	parameter2 "github.com/viant/xdatly/handler/parameter"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"sort"
	"strings"
	"sync"
)

var boolType = reflect.TypeOf(true)

type (
	Template struct {
		Source    string  `json:",omitempty" yaml:"source,omitempty"`
		SourceURL string  `json:",omitempty" yaml:"sourceURL,omitempty"`
		Schema    *Schema `json:",omitempty" yaml:"schema,omitempty"`
		stateType *structology.StateType

		PresenceSchema *Schema       `json:",omitempty" yaml:"presenceSchema,omitempty"`
		Parameters     Parameters    `json:",omitempty" yaml:"parameters,omitempty"`
		Meta           *TemplateMeta `json:",omitempty" yaml:",omitempty"`

		sqlEvaluator *expand.Evaluator

		accessors        *types.Accessors
		_parametersIndex NamedParameters
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

	t._parametersIndex = t.Parameters.Index()
	sort.Sort(t.Parameters)
	return t.initMetaIfNeeded(ctx, resource)
}

func (t *Template) updateSource(view *View) {
	if t.Source != "" {
		t.Source = "( " + t.Source + " )"
		return
	}

	t.Source = view.Source()
}

const defaultParameterPackage = "github.com/viant/datly/view/parameter"

func (t *Template) Package() string {
	if t.Schema != nil && t.Schema.Package != "" {
		return t.Schema.Package
	}
	if t._view != nil && t._view.Schema != nil && t._view.Schema.Package != "" {
		return t._view.Schema.Package
	}
	return defaultParameterPackage
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

	rType, err := t.Parameters.ReflectType(t.Package(), t._view._resource.LookupType(), true)
	//rType, err := BuildType(t.Parameters)
	if err != nil {
		return fmt.Errorf("failed to build template %s reflect type: %w", t._view.Name, err)
	}
	t.Schema = &Schema{}
	t.Schema.SetType(reflect.PtrTo(rType))
	t.stateType = structology.NewStateType(rType)
	return nil
}

func BuildType(parameters []*Parameter) (reflect.Type, error) {
	return buildType(parameters, nil)
}

func BuildTypeWithPresence(parameters []*Parameter) (reflect.Type, error) {
	presenceType, err := BuildPresenceType(parameters)
	if err != nil {
		return nil, err
	}

	return buildType(parameters, nil, reflect.StructField{
		Name: "Has",
		Type: reflect.PtrTo(presenceType),
		Tag:  `sqlx:"-" setMarker:"true"`,
	})
}

func buildType(parameters []*Parameter, paramType reflect.Type, fields ...reflect.StructField) (reflect.Type, error) {
	builder := parameter.NewBuilder("")
	for _, param := range parameters {
		pType := param.ActualParamType()
		if paramType != nil {
			pType = paramType
		}

		paramTag := reflect.StructTag(param.Tag)
		if err := builder.AddType(param.Name, pType, paramTag); err != nil {
			return nil, err
		}
	}

	for _, field := range fields {
		if err := builder.AddType(field.Name, field.Type, field.Tag); err != nil {
			return nil, err
		}
	}

	return builder.Build(), nil
}

func BuildPresenceType(parameters []*Parameter) (reflect.Type, error) {
	return buildType(parameters, xreflect.BoolType)
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

		aResource := &resourcelet{View: t._view, Resource: resource}
		if err := param.Init(ctx, aResource); err != nil {
			return err
		}
	}

	return nil
}

func NewEvaluator(parameters []*Parameter, paramSchema, presenceSchema reflect.Type, template string, typeLookup xreflect.LookupType, predicates []*expand.PredicateConfig) (*expand.Evaluator, error) {
	return expand.NewEvaluator(
		template,
		expand.WithConstUpdaters(FilterConstParameters(parameters)),
		expand.WithTypeLookup(typeLookup),
		expand.WithParamSchema(paramSchema, presenceSchema),
		expand.WithPredicates(predicates),
	)
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
	return t.EvaluateStateWithSession(externalParams, presenceMap, parentParam, batchData, nil, options...)
}

func (t *Template) EvaluateStateWithSession(externalParams interface{}, presenceMap interface{}, parentParam *expand.MetaParam, batchData *BatchData, sess *session.Session, options ...interface{}) (*expand.State, error) {
	var expander expand.Expander
	var dataUnit *expand.DataUnit
	for _, option := range options {
		switch actual := option.(type) {
		case expand.Expander:
			expander = actual
		case *expand.DataUnit:
			dataUnit = actual
		}
	}

	ops := []expand.StateOption{
		expand.WithParameters(externalParams, presenceMap),
		expand.WithViewParam(AsViewParam(t._view, nil, batchData, expander)),
		expand.WithParentViewParam(parentParam),
		expand.WithSession(sess),
	}

	if dataUnit != nil {
		ops = append(ops, expand.WithDataUnit(dataUnit))
	}

	return Evaluate(
		t.sqlEvaluator,
		ops...,
	)
}

// WithTemplateParameter return parameter template options
func WithTemplateParameter(parameter *Parameter) TemplateOption {
	return func(t *Template) {
		t.Parameters = append(t.Parameters, parameter)
	}
}

// NewTemplate creates a template
func NewTemplate(source string, opts ...TemplateOption) *Template {
	ret := &Template{Source: source}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func Evaluate(evaluator *expand.Evaluator, options ...expand.StateOption) (*expand.State, error) {
	return evaluator.Evaluate(nil,
		options...,
	)
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
	aResource := &resourcelet{View: t._view, Resource: resource}
	return param.Init(ctx, aResource)
}

func (t *Template) initSqlEvaluator(resource *Resource) error {
	if t.wasEmpty {
		return nil
	}

	cache := &predicateCache{Map: sync.Map{}}
	var predicates []*expand.PredicateConfig
	for _, p := range t.Parameters {
		for _, predicate := range p.Predicates {
			evaluator, err := cache.get(predicate, p, resource._predicates, t.Schema.Type(), t.PresenceSchema.Type())
			if err != nil {
				return err
			}

			predicates = append(predicates, &expand.PredicateConfig{
				Ensure:        predicate.Ensure,
				Context:       predicate.Context,
				StateAccessor: p.accessValue,
				HasAccessor:   p.accessHas,
				Expander: func(c *expand.Context, state, has, param interface{}) (*parameter2.Criteria, error) {
					evaluate, err := evaluator.Evaluate(c, state, has, param)
					if err != nil {
						return nil, err
					}

					return &parameter2.Criteria{Query: evaluate.Buffer.String()}, nil
				},
			})
		}
	}

	evaluator, err := NewEvaluator(t.Parameters, t.Schema.Type(), t.PresenceSchema.Type(), t.Source, resource.LookupType(), predicates)
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
	rType, err := BuildPresenceType(nonStateParameters(t.Parameters))
	if err != nil {
		return err
	}

	t.PresenceSchema = &Schema{}
	t.PresenceSchema.SetType(rType)

	return nil
}

func nonStateParameters(parameters []*Parameter) []*Parameter {
	params := make([]*Parameter, 0, len(parameters))
	for _, p := range parameters {
		params = append(params, p)
	}
	return params
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

		return nil, fmt.Errorf("not found field %v at type %v", name, structType.String())
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
