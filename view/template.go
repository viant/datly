package view

import (
	"context"
	"fmt"
	expand2 "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/service/executor/extension"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/template"
	"github.com/viant/structology"
	rdata "github.com/viant/toolbox/data"
	"github.com/viant/velty"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"sync"
)

var boolType = reflect.TypeOf(true)

type (
	Template struct {
		Source    string        `json:",omitempty" yaml:"source,omitempty"`
		SourceURL string        `json:",omitempty" yaml:"sourceURL,omitempty"`
		Schema    *state.Schema `json:",omitempty" yaml:"schema,omitempty"`
		stateType *structology.StateType

		Parameters state.Parameters `json:",omitempty" yaml:"parameters,omitempty"`
		Summary    *TemplateSummary `json:",omitempty" yaml:",omitempty"`

		sqlEvaluator *expand2.Evaluator

		_parametersIndex state.NamedParameters
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

func (t *Template) StateType() *structology.StateType {
	return t.stateType
}

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
	if rType := t.Schema.Type(); rType != nil {
		t.stateType = structology.NewStateType(rType)
	}

	if err = t.updateParametersFields(); err != nil {
		return err
	}
	if err = t.initSqlEvaluator(resource); err != nil {
		return err
	}

	t._parametersIndex = t.Parameters.Index()
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
	if err != nil {
		return fmt.Errorf("failed to build template %s reflect type: %w", t._view.Name, err)
	}
	t.Schema = state.NewSchema(reflect.PtrTo(rType))
	return nil
}

func buildType(parameters []*state.Parameter, paramType reflect.Type, fields ...reflect.StructField) (reflect.Type, error) {
	builder := template.NewBuilder("")
	for _, param := range parameters {
		pType := param.OutputType()
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

func BuildPresenceType(parameters []*state.Parameter) (reflect.Type, error) {
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

		aResource := &Resourcelet{View: t._view, Resource: resource}
		if err := param.Init(ctx, aResource); err != nil {
			return err
		}
	}

	return nil
}

func NewEvaluator(parameters state.Parameters, stateType *structology.StateType, template string, typeLookup xreflect.LookupType, predicates []*expand2.PredicateConfig) (*expand2.Evaluator, error) {
	return expand2.NewEvaluator(
		template,
		expand2.WithSetLiteral(parameters.SetLiterals),
		expand2.WithTypeLookup(typeLookup),
		expand2.WithStateType(stateType),
		expand2.WithPredicates(predicates),
	)
}

func (t *Template) EvaluateSource(parameterState *structology.State, parentParam *expand2.MetaParam, batchData *BatchData, options ...interface{}) (*expand2.State, error) {
	if t.wasEmpty {
		return expand2.StateWithSQL(t.Source), nil
	}
	return t.EvaluateState(parameterState, parentParam, batchData, options...)
}

func (t *Template) EvaluateState(parameterState *structology.State, parentParam *expand2.MetaParam, batchData *BatchData, options ...interface{}) (*expand2.State, error) {
	return t.EvaluateStateWithSession(parameterState, parentParam, batchData, nil, options...)
}

func (t *Template) EvaluateStateWithSession(parameterState *structology.State, parentParam *expand2.MetaParam, batchData *BatchData, sess *extension.Session, options ...interface{}) (*expand2.State, error) {
	var expander expand2.Expander
	var dataUnit *expand2.DataUnit
	for _, option := range options {
		switch actual := option.(type) {
		case expand2.Expander:
			expander = actual
		case *expand2.DataUnit:
			dataUnit = actual
		}
	}

	ops := []expand2.StateOption{
		expand2.WithParameterState(parameterState),
		expand2.WithViewParam(AsViewParam(t._view, nil, batchData, expander)),
		expand2.WithParentViewParam(parentParam),
		expand2.WithSession(sess),
	}

	if dataUnit != nil {
		ops = append(ops, expand2.WithDataUnit(dataUnit))
	}

	return Evaluate(
		t.sqlEvaluator,
		ops...,
	)
}

// WithTemplateParameter return parameter template options
func WithTemplateParameter(parameter *state.Parameter) TemplateOption {
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

func Evaluate(evaluator *expand2.Evaluator, options ...expand2.StateOption) (*expand2.State, error) {
	return evaluator.Evaluate(nil,
		options...,
	)
}

func AsViewParam(aView *View, aSelector *Statelet, batchData *BatchData, options ...interface{}) *expand2.MetaParam {
	var metaSource expand2.MetaSource
	if aView != nil {
		metaSource = aView
	}

	var metaExtras expand2.MetaExtras
	if aSelector != nil {
		metaExtras = aSelector
	}

	var metaBatch expand2.MetaBatch
	if batchData != nil {
		metaBatch = batchData
	}

	return expand2.NewMetaParam(metaSource, metaExtras, metaBatch, options...)
}

func (t *Template) inheritAndInitParam(ctx context.Context, resource *Resource, param *state.Parameter) error {
	aResource := &Resourcelet{View: t._view, Resource: resource}
	return param.Init(ctx, aResource)
}

func (t *Template) initSqlEvaluator(resource *Resource) error {
	if t.wasEmpty {
		return nil
	}

	cache := &predicateCache{Map: sync.Map{}}
	var predicates []*expand2.PredicateConfig
	for _, p := range t.Parameters {
		for _, predicate := range p.Predicates {
			evaluator, err := cache.get(resource, predicate, p, resource._predicates, t.stateType)
			if err != nil {
				return err
			}

			if p.Selector() == nil {
				panic("selector should have been set")
			}

			predicates = append(predicates, &expand2.PredicateConfig{
				Ensure:   predicate.Ensure,
				Group:    predicate.Group,
				Selector: p.Selector(),
				Expander: evaluator,
			})
		}
	}

	evaluator, err := NewEvaluator(t.Parameters, t.stateType, t.Source, resource.LookupType(), predicates)
	if err != nil {
		return err
	}

	t.sqlEvaluator = evaluator
	return nil
}

func nonStateParameters(parameters []*state.Parameter) []*state.Parameter {
	params := make([]*state.Parameter, 0, len(parameters))
	for _, p := range parameters {
		params = append(params, p)
	}
	return params
}

func (t *Template) updateParametersFields() error {
	for _, param := range t.Parameters {
		param.SetSelector(t.stateType.Lookup(param.Name))
		if param.Selector() == nil {
			return fmt.Errorf("parametr %v is missing in state", param.Name)
		}
	}

	return nil
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

func (t *Template) Expand(placeholders *[]interface{}, SQL string, selector *Statelet, params CriteriaParam, batchData *BatchData, sanitized *expand2.DataUnit) (string, error) {
	values, err := template.Parse(SQL)
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

func (t *Template) prepareExpanded(value *template.Value, params CriteriaParam, selector *Statelet, batchData *BatchData, placeholders *[]interface{}, sanitized *expand2.DataUnit) (string, string, error) {
	key, val, err := t.replacementEntry(value.Key, params, selector, batchData, placeholders, sanitized)
	if err != nil {
		return "", "", err
	}

	return key, val, err
}

func (t *Template) replacementEntry(key string, params CriteriaParam, selector *Statelet, batchData *BatchData, placeholders *[]interface{}, sanitized *expand2.DataUnit) (string, string, error) {
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
		values, err := selector.Template.Values(key)
		if err != nil {
			return "", "", err
		}

		*placeholders = append(*placeholders, values...)
		actualKey, bindings := expand2.AsBindings(key, values)
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
	if t.Summary == nil {
		return nil
	}

	return t.Summary.Init(ctx, t, r)
}

func (t *Template) EvaluatorStateType() reflect.Type {
	return t.sqlEvaluator.Type()
}
