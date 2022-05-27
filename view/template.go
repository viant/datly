package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/parameter"
	"github.com/viant/velty"
	"github.com/viant/velty/est"
	"github.com/viant/xunsafe"
	"reflect"
)

var boolType = reflect.TypeOf(true)

const (
	paramsMetadataKey = "Has"
	paramsKey         = "Unsafe"
	viewKey           = "View"

	Pagination    = "$PAGINATION"
	Criteria      = "$CRITERIA"
	WhereCriteria = "$WHERE_CRITERIA"

	ColumnsIn             = "$COLUMN_IN"
	WhereColumnInPosition = "$WHERE_COLUMN_IN"
	AndColumnInPosition   = "$AND_COLUMN_IN"

	SelectorCriteria      = "$SELECTOR_CRITERIA"
	WhereSelectorCriteria = "$WHERE_SELECTOR_CRITERIA"
	AndSelectorCriteria   = "$AND_SELECTOR_CRITERIA"

	WherePrefix = "WHERE_"
	AndPrefix   = "AND_"
	OrPrefix    = "OR_"
)

type (
	Template struct {
		_view *View

		Source         string  `json:",omitempty"`
		SourceURL      string  `json:",omitempty"`
		Schema         *Schema `json:",omitempty"`
		PresenceSchema *Schema `json:",omitempty"`

		Parameters []*Parameter `json:",omitempty"`

		sqlEvaluator *Evaluator

		accessors        *Accessors
		_fields          []reflect.StructField
		_fieldIndex      map[string]int
		_parametersIndex ParametersIndex
		initialized      bool
	}

	Evaluator struct {
		planner       *velty.Planner
		executor      *est.Execution
		stateProvider func() *est.State
	}

	CommonParams struct {
		ColumnsIn   string `velty:"COLUMN_IN"`
		WhereClause string `velty:"CRITERIA"`
		Pagination  string `velty:"PAGINATION"`
	}

	Param struct {
		Name  string
		Alias string
		Table string
	}
)

func (t *Template) Init(ctx context.Context, resource *Resource, view *View) error {
	if t.initialized {
		return nil
	}
	err := t.loadSourceFromURL(ctx, resource)
	if err != nil {
		return err
	}
	t._view = view
	t.initialized = true
	t._parametersIndex = ParametersSlice(t.Parameters).Index()
	t._fieldIndex = map[string]int{}

	if t.Source != "" {
		t.Source = "( " + t.Source + " )"
	} else {
		t.Source = view.Source()
	}

	if err := t.initTypes(ctx, resource); err != nil {
		return err
	}

	if err := t.initPresenceType(resource); err != nil {
		return err
	}

	if err := t.initSqlEvaluator(); err != nil {
		return err
	}

	t.initAccessors()

	if err := t.updateParametersFields(); err != nil {
		return err
	}

	return nil
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
	t.Schema = &Schema{}

	builder := parameter.NewBuilder("")

	for _, param := range t.Parameters {
		if err := t.inheritAndInitParam(ctx, resource, param); err != nil {
			return err
		}

		if err := builder.AddType(param.Name, param.Schema.Type()); err != nil {
			return err
		}
	}

	t.Schema.setType(builder.Build())

	return nil
}

func (t *Template) addField(name string, rType reflect.Type) error {
	_, ok := t._fieldIndex[name]
	if ok {
		return fmt.Errorf("field with %v name already exists", name)
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
		t.Schema.setType(rType)
	}

	for _, param := range t.Parameters {
		if err := t.inheritAndInitParam(ctx, resource, param); err != nil {
			return err
		}

		if err := param.Init(ctx, resource, t.Schema.Type()); err != nil {
			return err
		}
	}

	return nil
}

func (t *Template) newEvaluator(template string) (*Evaluator, error) {
	evaluator := &Evaluator{}
	var err error

	evaluator.planner = velty.New(velty.BufferSize(len(template)))
	if err = evaluator.planner.DefineVariable(paramsKey, t.Schema.Type()); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(paramsMetadataKey, t.PresenceSchema.Type()); err != nil {
		return nil, err
	}

	if err = evaluator.planner.DefineVariable(viewKey, reflect.TypeOf(&Param{})); err != nil {
		return nil, err
	}

	evaluator.executor, evaluator.stateProvider, err = evaluator.planner.Compile([]byte(template))
	if err != nil {
		return nil, err
	}

	return evaluator, nil
}

func (t *Template) EvaluateSource(externalParams, presenceMap interface{}, parent *View) (string, error) {
	SQL, err := t.evaluate(t.sqlEvaluator, externalParams, presenceMap, parent)
	return SQL, err
}

func (t *Template) evaluate(evaluator *Evaluator, externalParams, presenceMap interface{}, parent *View) (string, error) {
	externalType := reflect.TypeOf(externalParams)
	if t.Schema.Type() != externalType {
		return "", fmt.Errorf("inompactible types, wanted %v got %T", t.Schema.Type().String(), externalParams)
	}

	newState := evaluator.stateProvider()
	if externalParams != nil {
		if err := newState.SetValue(paramsKey, externalParams); err != nil {
			return "", err
		}
	}

	if presenceMap != nil {
		if err := newState.SetValue(paramsMetadataKey, presenceMap); err != nil {
			return "", err
		}
	}

	viewParam := &Param{}
	if parent != nil {
		viewParam = asParam(parent)
	} else {
		viewParam = asParam(t._view)
	}

	if err := newState.SetValue(viewKey, viewParam); err != nil {
		return "", err
	}

	evaluator.executor.Exec(newState)
	return newState.Buffer.String(), nil
}

func asParam(parent *View) *Param {
	viewParam := &Param{
		Name:  parent.Name,
		Alias: parent.Alias,
		Table: parent.Table,
	}

	return viewParam
}

func (t *Template) inheritAndInitParam(ctx context.Context, resource *Resource, param *Parameter) error {
	return param.Init(ctx, resource, nil)
}

func (t *Template) initSqlEvaluator() error {
	evaluator, err := t.newEvaluator(t.Source)
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

	t.PresenceSchema.setType(rType)
	return nil
}

func (t *Template) initPresenceSchemaFromParams() error {
	builder := parameter.NewBuilder("")

	for _, param := range t.Parameters {
		if err := builder.AddType(param.PresenceName, boolType); err != nil {
			return err
		}
	}

	t.PresenceSchema = &Schema{}
	t.PresenceSchema.setType(builder.Build())

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
		t.accessors = &Accessors{index: map[string]int{}}
	}

	t.accessors.init(t.Schema.Type())
}

func (t *Template) AccessorByName(name string) (*Accessor, error) {
	i, ok := t.accessors.index[name]
	if !ok {
		return nil, fmt.Errorf("not found accessor for param %v", name)
	}

	return t.accessors.accessors[i], nil
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
