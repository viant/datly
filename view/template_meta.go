package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/template/expand"
	"github.com/viant/toolbox/format"
	"strings"
)

const (
	MetaTypeRecord MetaKind = "record"
	MetaTypeHeader MetaKind = "header"
)

type (
	MetaKind     string
	TemplateMeta struct {
		SourceURL   string
		Source      string
		Name        string
		Kind        MetaKind
		Cardinality Cardinality

		sqlEvaluator *expand.Evaluator
		Schema       *Schema
		_owner       *Template
		initialized  bool
	}
)

func (m *TemplateMeta) Init(ctx context.Context, owner *Template, resource *Resource) error {
	if m.initialized == true {
		return nil
	}

	m.Kind = MetaKind(strings.ToLower(string(m.Kind)))

	cFormat, err := format.NewCase(DetectCase(m.Name))
	if err == nil && cFormat != format.CaseUpperCamel {
		m.Name = cFormat.Format(m.Name, format.CaseUpperCamel)
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

	schemaDataType := FirstNotEmpty(m.Schema.DataType, m.Schema.Name)
	if schemaDataType != "" {
		dataType, err := GetOrParseType(resource.LookupType, schemaDataType)
		if err != nil {
			return err
		}

		m.Schema.SetType(dataType)
		return nil
	}

	columns, err := m.getColumns(ctx, resource, owner)
	if err != nil {
		return err
	}

	for _, column := range columns {
		if err = column.Init(resource, owner._view.Caser, owner._view.AreNullValuesAllowed(), nil); err != nil {
			return err
		}
	}

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

	m.Schema.initByColumns(columns, nil, nil, newCase)
	return nil
}

func (m *TemplateMeta) getColumns(ctx context.Context, resource *Resource, owner *Template) ([]*Column, error) {
	if resource._columnsCache != nil {
		columns, ok := resource._columnsCache[m.newMetaColumnsCacheKey()]
		if ok {
			return columns, nil
		}

		columns, ok = resource._columnsCache[m.oldMetaColumnsCacheKey()]
		if ok {
			return columns, nil
		}
	}

	SQL, args, err := m.prepareSQL(owner)
	if err != nil {
		return nil, err
	}

	columns, _, err := detectColumns(ctx, &TemplateEvaluation{
		SQL:       SQL,
		Evaluated: true,
		Expander:  owner.Expand,
		Args:      args,
	}, owner._view)

	if err != nil {
		return nil, err
	}

	if resource._columnsCache != nil {
		resource._columnsCache[m.newMetaColumnsCacheKey()] = columns
	}

	return columns, nil
}

func (m *TemplateMeta) newMetaColumnsCacheKey() string {
	return "view: " + m._owner._view.Name + "template_meta:" + m.Name
}

//Deprecated: oldMetaColumnsCacheKey is deprecated.
func (m *TemplateMeta) oldMetaColumnsCacheKey() string {
	return "template_meta:" + m.Name
}

func (m *TemplateMeta) prepareSQL(owner *Template) (string, []interface{}, error) {
	selectorValues := expand.NewValue(owner.Schema.Type())
	selectorPresence := expand.NewValue(owner.PresenceSchema.Type())
	viewParam := AsViewParam(owner._view, nil, nil)

	state, sanitizer, _, err := Evaluate(owner.sqlEvaluator, selectorValues, selectorPresence, viewParam, nil)
	if err != nil {
		return "", nil, err
	}

	viewParam.NonWindowSQL = ExpandWithFalseCondition(state.Buffer.String())
	viewParam.Args = sanitizer.ParamsGroup
	return m.Evaluate(selectorValues, selectorPresence, viewParam)
}

func (m *TemplateMeta) Evaluate(selectorValues interface{}, selectorPresence interface{}, viewParam *expand.MetaParam) (string, []interface{}, error) {
	state, sanitizer, _, err := Evaluate(m.sqlEvaluator, selectorValues, selectorPresence, viewParam, nil)
	if err != nil {
		return "", nil, err
	}

	return state.Buffer.String(), sanitizer.ParamsGroup, nil
}

func (m *TemplateMeta) initTemplateEvaluator(_ context.Context, owner *Template, resource *Resource) error {
	evaluator, err := NewEvaluator(owner.Parameters, owner.Schema.Type(), owner.PresenceSchema.Type(), m.Source, resource.LookupType)
	if err != nil {
		return err
	}

	m.sqlEvaluator = evaluator
	return nil
}
