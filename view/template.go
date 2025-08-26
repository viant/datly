package view

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	expand "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/service/executor/extension"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/template"
	"github.com/viant/structology"
	rdata "github.com/viant/toolbox/data"
	"github.com/viant/xreflect"
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

		sqlEvaluator *expand.Evaluator

		_parametersIndex state.NamedParameters
		initialized      bool
		isTemplate       bool
		wasEmpty         bool
		_view            *View
	}

	TemplateOption func(t *Template)

	CriteriaParam struct {
		ColumnsIn             string `velty:"COLUMN_IN"`
		WhereClause           string `velty:"CRITERIA"`
		WhereClauseParameters []interface{}
		Pagination            string `velty:"PAGINATION"`
	}
)

func (t *Template) StateType() *structology.StateType {
	return t.stateType
}

func (t *Template) Init(ctx context.Context, resource *Resource, view *View) error {
	if t.initialized {
		return nil
	}
	if view._resource != nil {
		resource = view._resource
	}
	t.wasEmpty = t.Source == "" && t.SourceURL == ""
	t.initialized = true

	err := t.loadSourceFromURL(ctx, resource)
	if err != nil {
		return err
	}

	if len(resource.Substitutes) > 0 {
		t.Source = resource.ExpandSubstitutes(t.Source)
	}

	t._view = view
	t.updateSource(view)

	t.isTemplate = t.Source != view.Name && t.Source != view.Table
	//todo try allocate parameters based on group
	if len(t.Parameters) == 0 {

		if strings.Contains(t.Source, "${predicate.Builder()") {
			for i, candidate := range resource.Parameters {
				if len(candidate.Predicates) > 0 {
					t.Parameters = append(t.Parameters, state.NewRefParameter(resource.Parameters[i].Name))
				}
			}

		}
	}

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
	rType, err := t.Parameters.ReflectType(t.Package(), t._view._resource.LookupType(), state.WithSetMarker())
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

func NewEvaluator(parameters state.Parameters, stateType *structology.StateType, template string, typeLookup xreflect.LookupType, predicates []*expand.PredicateConfig) (*expand.Evaluator, error) {
	return expand.NewEvaluator(
		template,
		expand.WithSetLiteral(parameters.SetLiterals),
		expand.WithTypeLookup(typeLookup),
		expand.WithStateType(stateType),
		expand.WithPredicates(predicates),
	)
}

func (t *Template) EvaluateSource(ctx context.Context, parameterState *structology.State, parentParam *expand.ViewContext, batchData *BatchData, options ...interface{}) (*expand.State, error) {
	if t.wasEmpty {
		return expand.StateWithSQL(ctx, t.Source), nil
	}
	return t.EvaluateState(ctx, parameterState, parentParam, batchData, options...)
}

func (t *Template) EvaluateState(ctx context.Context, parameterState *structology.State, parentParam *expand.ViewContext, batchData *BatchData, options ...interface{}) (*expand.State, error) {
	return t.EvaluateStateWithSession(ctx, parameterState, parentParam, batchData, nil, options...)
}

func (t *Template) EvaluateStateWithSession(ctx context.Context, parameterState *structology.State, parentParam *expand.ViewContext, batchData *BatchData, sess *extension.Session, options ...interface{}) (*expand.State, error) {
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
		expand.WithParameterState(parameterState),
		expand.WithViewParam(AsViewParam(t._view, nil, batchData, expander)),
		expand.WithParentViewParam(parentParam),
		expand.WithSession(sess),
	}

	if dataUnit != nil {
		ops = append(ops, expand.WithDataUnit(dataUnit))
	}
	return Evaluate(ctx,
		t.sqlEvaluator,
		ops...,
	)
}

// WithTemplateParameters return parameter template options
func WithTemplateParameters(parameters ...*state.Parameter) TemplateOption {
	return func(t *Template) {
		t.Parameters = append(t.Parameters, parameters...)
	}
}

// WithTemplateSchema returns with template schema
func WithTemplateSchema(schema *state.Schema) TemplateOption {
	return func(t *Template) {
		t.Schema = schema
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

func Evaluate(ctx context.Context, evaluator *expand.Evaluator, options ...expand.StateOption) (*expand.State, error) {
	return evaluator.Evaluate(&expand.Context{Context: ctx},
		options...,
	)
}

func AsViewParam(aView *View, aSelector *Statelet, batchData *BatchData, options ...interface{}) *expand.ViewContext {
	var metaSource expand.ParentSource
	if aView != nil {
		metaSource = aView
	}

	var metaExtras expand.ParentExtras
	if aSelector != nil {
		metaExtras = aSelector
	}

	var metaBatch expand.ParentBatch
	if batchData != nil {
		metaBatch = batchData
	}

	return expand.NewViewContext(metaSource, metaExtras, metaBatch, options...)
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
	var predicates []*expand.PredicateConfig
	for _, p := range t.Parameters {
		for _, predicate := range p.Predicates {
			evaluator, err := cache.get(resource, predicate, p, resource._predicates, t.stateType)
			if err != nil {
				return err
			}

			if p.Selector() == nil {
				panic("selector should have been set")
			}

			predicates = append(predicates, &expand.PredicateConfig{
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

func (t *Template) updateParametersFields() error {
	for _, param := range t.Parameters {
		param.SetSelector(t.stateType.Lookup(param.Name))
		if param.Selector() == nil {
			return fmt.Errorf("parametr %v is missing in state", param.Name)
		}
	}

	return nil
}

func (t *Template) IsActualTemplate() bool {
	return t.isTemplate
}

func (t *Template) Expand(placeholders *[]interface{}, SQL string, selector *Statelet, params CriteriaParam, batchData *BatchData, sanitized *expand.DataUnit) (string, error) {
	values, err := template.Parse(SQL)
	if err != nil {
		return "", err
	}
	replacement := &rdata.Map{}
	for _, value := range values {
		if value.Key == "?" {
			placeholder, err := sanitized.Next()
			if err != nil {
				return "", fmt.Errorf("failed to get placeholder: %w, SQL: %v, values: %+v\n", err, SQL, values)
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

func (t *Template) prepareExpanded(value *template.Value, params CriteriaParam, selector *Statelet, batchData *BatchData, placeholders *[]interface{}, sanitized *expand.DataUnit) (string, string, error) {
	key, val, err := t.replacementEntry(value.Key, params, selector, batchData, placeholders, sanitized)
	if err != nil {
		return "", "", err
	}

	return key, val, err
}

func (t *Template) replacementEntry(key string, params CriteriaParam, selector *Statelet, batchData *BatchData, placeholders *[]interface{}, sanitized *expand.DataUnit) (string, string, error) {
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
		if len(params.WhereClauseParameters) > 0 {
			for i := range params.WhereClauseParameters {
				if _, err := sanitized.Add(0, params.WhereClauseParameters[i]); err != nil {
					return "", "", err
				}
			}
			params.WhereClauseParameters = nil
		}
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
	if t.Summary == nil {
		return nil
	}
	return t.Summary.Init(ctx, t, r)
}

func (t *Template) EvaluatorStateType() reflect.Type {
	return t.sqlEvaluator.Type()
}
