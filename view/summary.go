package view

import (
	"context"
	"fmt"
	expand "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/sqlx/io/read/cache/ast"
	"github.com/viant/structology"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

const (
	MetaKindRecord MetaKind = "record"
	MetaKindHeader MetaKind = "header"
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

	cFormat := text.DetectCaseFormat(m.Name)
	if cFormat.IsDefined() && cFormat != text.CaseFormatUpperCamel {
		m.Name = cFormat.Format(m.Name, text.CaseFormatUpperCamel)
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
	if err := m.Schema.LoadTypeIfNeeded(resource.LookupType()); err != nil && m.Schema.Type() != nil {
		return err
	}
	source := m._owner._view
	if parent := source._parent; parent != nil && m.Kind == MetaKindRecord {
		if compType := parent.Schema.CompType(); compType != nil {
			compType = types.EnsureStruct(compType)
			field, ok := compType.FieldByName(m.Name)
			if !ok {
				return fmt.Errorf("invalid view summary:'%s', field %s is missing in the view '%s' schema ", m.Name, m.Name, compType.String())
			}
			m.Schema.SetType(field.Type)
			fmt.Printf("SET SuMMARY: %s\n", field.Type.String())
			return nil
		}
	}

	columns, err := m.getColumns(ctx, resource, owner)
	if err != nil {
		return err
	}
	resourcelet := NewResources(resource, owner._view)
	if err = columns.Init(resourcelet, owner._view.CaseFormat, owner._view.AreNullValuesAllowed()); err != nil {
		return err
	}
	if err != nil {
		return fmt.Errorf("couldn't resolve template meta SQL due to the: %w", err)
	}
	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = column.Name
	}
	caseFormat := text.DetectCaseFormat(columnNames...)
	m.Schema = state.NewSchema(nil, state.WithAutoGenFunc(m._owner._view.generateSchemaTypeFromColumn(caseFormat, columns, nil)))
	err = m.Schema.Init(resourcelet)

	return err
}

func (v *View) generateSchemaTypeFromColumn(caseFormat text.CaseFormat, columns []*Column, relations []*Relation) func() (reflect.Type, error) {
	return ColumnsSchema(caseFormat, columns, relations, v)
}

func ColumnsSchema(caseFormat text.CaseFormat, columns []*Column, relations []*Relation, v *View) func() (reflect.Type, error) {
	return ColumnsSchemaDocumented(caseFormat, columns, relations, v, nil)
}

func ColumnsSchemaDocumented(caseFormat text.CaseFormat, columns []*Column, relations []*Relation, v *View, doc state.Documentation) func() (reflect.Type, error) {
	return func() (reflect.Type, error) {
		excluded := make(map[string]bool)
		for _, rel := range relations {
			for _, item := range rel.On {
				if !rel.IncludeColumn && rel.Cardinality == state.One {
					excluded[item.Column] = true
				}
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
				columns[i].Codec.Init(v.Resource(), columns[i].rType)
				rType = columns[i].Codec.Schema.Type()
				if rType == nil {
					rType = xreflect.InterfaceType
				}
			}

			aTag := generateFieldTag(columns[i], caseFormat, doc, v.Table)
			aField := newCasedField(aTag, columnName, caseFormat, rType)
			if unique[aField.Name] {
				continue
			}
			unique[aField.Name] = true
			structFields = append(structFields, aField)
		}

		holders := make(map[string]bool)
		v.buildRelationField(relations, holders, &structFields)

		if v.SelfReference != nil {
			structFields = append(structFields, newCasedField("", v.SelfReference.Holder, text.CaseFormatUpperCamel, reflect.SliceOf(ast.InterfaceType)))
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

		aTag := tags.Tag{}
		aTag.TypeName = rel.Of.Schema.Name
		aTag.LinkOn = rel.TagLink()
		aTag.SQL = tags.NewViewSQL(rel.Of.View.Template.Source, "")
		if aBatch := rel.Of.View.Batch; aBatch != nil {
			if aTag.View == nil {
				aTag.View = &tags.View{}
			}
			aTag.View.Batch = aBatch.Size

		}
		if partitioned := rel.Of.View.Partitioned; partitioned != nil {
			if aTag.View == nil {
				aTag.View = &tags.View{}
			}
			aTag.View.PartitionerType = partitioned.DataType
			aTag.View.PartitionedConcurrency = partitioned.Concurrency

		}
		holderConnector := v.Connector
		if connector := rel.Of.Connector; connector != nil {
			if connector.Ref != holderConnector.Ref {
				aTag.View = &tags.View{Connector: connector.Ref}
			}
		}
		if rel.Of.MatchStrategy != "" {
			if aTag.View == nil {
				aTag.View = &tags.View{}
			}
			aTag.View.Match = string(rel.Of.MatchStrategy)

		}
		fieldTag := aTag.UpdateTag(``)

		holders[rel.Holder] = true
		*structFields = append(*structFields, reflect.StructField{
			Name: rel.Holder,
			Type: rType,
			Tag:  fieldTag,
		})

		if meta := rel.Of.View.Template.Summary; meta != nil {
			metaType := meta.Schema.Type()
			if metaType.Kind() != reflect.Ptr {
				metaType = reflect.PtrTo(metaType)
			}

			typeNameTag := getTypenameTag(meta.Schema.Name)
			tag := `json:",omitempty" yaml:",omitempty" sqlx:"-" ` + typeNameTag
			*structFields = append(*structFields, newCasedField(tag, meta.Name, text.CaseFormatUpperCamel, metaType))
		}
	}
}

// DefaultTypeName returns a default view type name
func DefaultTypeName(name string) string {
	if name == "" {
		return name
	}
	name = state.SanitizeTypeName(name)
	return name + "View"
}

func getTypenameTag(typeName string) string {
	if typeName == "" {
		return ""
	}
	typeName = state.SanitizeTypeName(typeName)
	return " " + xreflect.TagTypeName + `:"` + typeName + `"`
}

func newCasedField(aTag string, columnName string, sourceCaseFormat text.CaseFormat, rType reflect.Type) reflect.StructField {
	structFieldName := state.StructFieldName(sourceCaseFormat, columnName)
	return state.NewField(aTag, structFieldName, rType)
}

func (m *TemplateSummary) getColumns(ctx context.Context, resource *Resource, owner *Template) (Columns, error) {
	if resource.viewColumns != nil {
		columns, ok := resource.viewColumns[m.newMetaColumnsCacheKey()]
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

	if resource.viewColumns != nil {
		resource.viewColumns[m.newMetaColumnsCacheKey()] = columns
	}

	return columns, nil
}

func (m *TemplateSummary) newMetaColumnsCacheKey() string {
	return SummaryViewKey(m._owner._view.Name, m.Name)
}

// SummaryViewKey returns template summary key
func SummaryViewKey(ownerName, name string) string {
	return ownerName + "/DataSummary/" + name
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

func (m *TemplateSummary) Evaluate(parameterState *structology.State, viewParam *expand.ViewContext) (string, []interface{}, error) {
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
