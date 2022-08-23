package view

import (
	"context"
	"fmt"
	"github.com/viant/toolbox/format"
)

const (
	RecordTemplateMetaKind TemplateMetaKind = "record"
	HeaderTemplateMetaKind TemplateMetaKind = "header"
)

type (
	TemplateMetaKind string
	TemplateMeta     struct {
		SourceURL   string
		Source      string
		Name        string
		Kind        TemplateMetaKind
		Cardinality Cardinality

		sqlEvaluator *Evaluator
		Schema       *Schema
		_owner       *Template
		initialized  bool
	}
)

func (m *TemplateMeta) Init(ctx context.Context, owner *Template, resource *Resource) error {
	if m.initialized == true {
		return nil
	}

	if m.Name == "" {
		return fmt.Errorf("template meta name can't be empty")
	}

	if m.SourceURL != "" {
		template, err := resource.LoadText(ctx, m.SourceURL)
		if err != nil {
			return err
		}

		m.Source = template
	}

	if m.Source == "" {
		return fmt.Errorf("template meta Source or SourceURL can't be empty")
	}

	m.initialized = true
	m._owner = owner

	if err := m.initTemplateEvaluator(ctx, owner, resource); err != nil {
		return err
	}

	if err := m.initSchemaIfNeeded(ctx, owner, resource); err != nil {
		return err
	}

	return nil
}

func (m *TemplateMeta) initSchemaIfNeeded(ctx context.Context, owner *Template, resource *Resource) error {
	if m.Schema == nil {
		m.Schema = &Schema{}
	}

	if m.Schema.DataType != "" {
		dataType, err := GetOrParseType(resource._types, m.Schema.DataType)
		if err != nil {
			return err
		}

		m.Schema.setType(dataType)
		return nil
	}

	SQL, args, err := m.prepareSQL(owner)
	if err != nil {
		return err
	}

	columns, _, err := detectColumns(ctx, &TemplateEvaluation{
		SQL:       SQL,
		Evaluated: true,
		Expander:  owner.Expand,
		Args:      args,
	}, owner._view)

	if err != nil {
		return fmt.Errorf("couldn't resolve template meta SQL due to the: %w", err)
	}

	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}

	newCase, err := format.NewCase(DetectCase(columnNames...))
	if err != nil {
		return err
	}

	m.Schema.initByColumns(columns, nil, newCase)
	return nil
}

func (m *TemplateMeta) prepareSQL(owner *Template) (string, []interface{}, error) {
	selectorValues := newValue(owner.Schema.Type())
	selectorPresence := newValue(owner.PresenceSchema.Type())
	viewParam := AsViewParam(owner._view, nil)

	templateSQL, sanitizer, _, err := Evaluate(owner.sqlEvaluator, owner.Schema.Type(), selectorValues, selectorPresence, viewParam, nil)
	if err != nil {
		return "", nil, err
	}

	viewParam.NonWindowSQL = ExpandWithFalseCondition(templateSQL)
	viewParam.Args = sanitizer.ParamsGroup
	return m.Evaluate(selectorValues, selectorPresence, viewParam)
}

func (m *TemplateMeta) Evaluate(selectorValues interface{}, selectorPresence interface{}, viewParam *MetaParam) (string, []interface{}, error) {
	SQL, sanitizer, _, err := Evaluate(m.sqlEvaluator, m._owner.Schema.Type(), selectorValues, selectorPresence, viewParam, nil)
	if err != nil {
		return "", nil, err
	}

	return SQL, sanitizer.ParamsGroup, nil
}

func (m *TemplateMeta) initTemplateEvaluator(_ context.Context, owner *Template, _ *Resource) error {
	evaluator, err := NewEvaluator(owner.Schema.Type(), owner.PresenceSchema.Type(), m.Source)
	if err != nil {
		return err
	}

	m.sqlEvaluator = evaluator
	return nil
}
