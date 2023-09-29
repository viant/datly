package view

import (
	"context"
	"fmt"
	expand "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlx/io/read/cache/ast"
	"github.com/viant/structology"
	"github.com/viant/toolbox/format"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

const (
	MetaTypeRecord MetaKind = "record"
	MetaTypeHeader MetaKind = "header"
)

type (
	MetaKind        string
	TemplateSummary struct {
		SourceURL    string
		Source       string
		Name         string
		Kind         MetaKind
		Cardinality  state.Cardinality
		Columns      Columns
		sqlEvaluator *expand.Evaluator
		Schema       *state.Schema
		_owner       *Template
		initialized  bool
	}
)

func (m *TemplateSummary) EnsureSchema() {
	if m.Schema != nil {
		return
	}
	m.Schema = &state.Schema{}
}

func (m *TemplateSummary) Init(ctx context.Context, owner *Template, resource *Resource) error {
	if m.initialized == true {
		return nil
	}

	m.Kind = MetaKind(strings.ToLower(string(m.Kind)))

	cFormat, err := format.NewCase(formatter.DetectCase(m.Name))
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
		return fmt.Errorf("template meta Source or Source can't be empty")
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

func (m *TemplateSummary) initSchemaIfNeeded(ctx context.Context, owner *Template, resource *Resource) error {
	if m.Schema == nil {
		m.Schema = &state.Schema{}
	}
	if typeName := m.Schema.TypeName(); typeName != "" {
		dataType, err := types.LookupType(resource.LookupType(), typeName)
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
	resourcelet := NewResourcelet(resource, owner._view)
	if err = columns.Init(resourcelet, owner._view.Caser, owner._view.AreNullValuesAllowed()); err != nil {
		return err
	}
	if err != nil {
		return fmt.Errorf("couldn't resolve template meta SQL due to the: %w", err)
	}
	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}
	newCase, err := format.NewCase(formatter.DetectCase(columnNames...))
	if err != nil {
		return err
	}
	m.Schema = state.NewSchema(nil, state.WithAutoGenFunc(m._owner._view.generateSchemaTypeFromColumn(newCase, columns, nil)))
	err = m.Schema.Init(resourcelet)
	return err
}

func (v *View) generateSchemaTypeFromColumn(caser format.Case, columns []*Column, relations []*Relation) func() (reflect.Type, error) {
	return ColumnsSchema(caser, columns, relations, v)
}

func ColumnsSchema(caser format.Case, columns []*Column, relations []*Relation, v *View) func() (reflect.Type, error) {
	return func() (reflect.Type, error) {
		excluded := make(map[string]bool)
		for _, rel := range relations {
			if !rel.IncludeColumn && rel.Cardinality == state.One {
				excluded[rel.Column] = true
			}
		}

		fieldsLen := len(columns)
		structFields := make([]reflect.StructField, 0)
		unique := map[string]bool{}
		for i := 0; i < fieldsLen; i++ {
			columnName := columns[i].Name
			if _, ok := excluded[columnName]; ok {
				continue
			}

			rType := columns[i].ColumnType()
			if columns[i].Nullable && rType.Kind() != reflect.Ptr {
				rType = reflect.PtrTo(rType)
			}
			if columns[i].Codec != nil {
				rType = columns[i].Codec.Schema.Type()
				if rType == nil {
					rType = xreflect.InterfaceType
				}
			}

			aTag := generateFieldTag(columns[i], caser)
			aField := newCasedField(aTag, columnName, caser, rType)
			if unique[aField.Name] {
				continue
			}
			unique[aField.Name] = true
			structFields = append(structFields, aField)
		}

		holders := make(map[string]bool)
		v.buildRelationField(relations, holders, &structFields)

		if v.SelfReference != nil {
			structFields = append(structFields, newCasedField("", v.SelfReference.Holder, format.CaseUpperCamel, reflect.SliceOf(ast.InterfaceType)))
		}
		return reflect.PtrTo(reflect.StructOf(structFields)), nil
	}
}

func (v *View) buildRelationField(relations []*Relation, holders map[string]bool, structFields *[]reflect.StructField) {
	if len(relations) == 0 {
		return
	}
	for _, rel := range relations {
		if _, ok := holders[rel.Holder]; ok {
			continue
		}

		rType := rel.Of.DataType()
		if rType.Kind() == reflect.Struct {
			rType = reflect.PtrTo(rType)
			rel.Of.Schema.SetType(rType)
		}

		if rel.Cardinality == state.Many && rType.Kind() != reflect.Slice {
			rType = reflect.SliceOf(rType)
		}

		var fieldTag string
		if v.Async != nil {
			if v.Async.MarshalRelations {
				fieldTag = AsyncTagName + `:"enc=JSON" jsonx:"rawJSON"`
			} else {
				fieldTag = AsyncTagName + `:"table=` + v.Async.Table + `"`
			}
		}

		holders[rel.Holder] = true
		*structFields = append(*structFields, reflect.StructField{
			Name: rel.Holder,
			Type: rType,
			Tag:  reflect.StructTag(fieldTag),
		})

		if meta := rel.Of.View.Template.Summary; meta != nil {
			metaType := meta.Schema.Type()
			if metaType.Kind() != reflect.Ptr {
				metaType = reflect.PtrTo(metaType)
			}
			tag := `json:",omitempty" yaml:",omitempty" sqlx:"-"`
			*structFields = append(*structFields, newCasedField(tag, meta.Name, format.CaseUpperCamel, metaType))
		}
	}
}

func newCasedField(aTag string, columnName string, sourceCaseFormat format.Case, rType reflect.Type) reflect.StructField {
	structFieldName := state.StructFieldName(sourceCaseFormat, columnName)
	return state.NewField(aTag, structFieldName, rType)
}

func (m *TemplateSummary) getColumns(ctx context.Context, resource *Resource, owner *Template) (Columns, error) {
	if resource._columnsCache != nil {
		columns, ok := resource._columnsCache[m.newMetaColumnsCacheKey()]
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

func (m *TemplateSummary) newMetaColumnsCacheKey() string {
	return SummaryViewKey(m._owner._view.Name, m.Name)
}

// SummaryViewKey returns template summary key
func SummaryViewKey(ownerName, name string) string {
	return ownerName + "/Summary/" + name
}

func (m *TemplateSummary) prepareSQL(owner *Template) (string, []interface{}, error) {
	stateValue := owner.stateType.NewState()

	viewParam := AsViewParam(owner._view, nil, nil)

	state, err := Evaluate(owner.sqlEvaluator, expand.WithParameterState(stateValue), expand.WithViewParam(viewParam))
	if err != nil {
		return "", nil, err
	}

	viewParam.NonWindowSQL = ExpandWithFalseCondition(state.Buffer.String())
	viewParam.Args = state.DataUnit.ParamsGroup
	return m.Evaluate(stateValue, viewParam)
}

func (m *TemplateSummary) Evaluate(parameterState *structology.State, viewParam *expand.MetaParam) (string, []interface{}, error) {
	state, err := Evaluate(m.sqlEvaluator, expand.WithParameterState(parameterState), expand.WithViewParam(viewParam))
	if err != nil {
		return "", nil, err
	}

	return state.Buffer.String(), state.DataUnit.ParamsGroup, nil
}

func (m *TemplateSummary) initTemplateEvaluator(_ context.Context, owner *Template, resource *Resource) error {
	evaluator, err := NewEvaluator(owner.Parameters, owner.stateType, m.Source, resource.LookupType(), nil)
	if err != nil {
		return err
	}
	m.sqlEvaluator = evaluator
	return nil
}
