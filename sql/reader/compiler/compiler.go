package compiler

import (
	"fmt"
	"io/fs"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	sqlmacro "github.com/viant/datly/sql/macro"
	sqlreader "github.com/viant/datly/sql/reader"
	rcollector "github.com/viant/datly/sql/reader/collector"
	"github.com/viant/datly/sql/reader/rowcodec"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
	xcodec "github.com/viant/xdatly/codec"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xreflect"
)

type Input struct {
	CodecFactory    xcodec.Factory
	Component       *spec.Component
	InputType       reflect.Type
	OutputType      reflect.Type
	Bindings        []bindly.BindingSpec
	Predicate       dexec.PredicateEvaluator
	TypeLookup      func(name string) (reflect.Type, error)
	DirectViewField string
	DirectViewType  reflect.Type
	Resources       fs.FS
}

func Compile(input Input) (*sqlreader.Plan, error) {
	return (&planCompiler{input: input}).Compile()
}

type planCompiler struct {
	input Input
}

func (b *planCompiler) Compile() (*sqlreader.Plan, error) {
	directViewField := strings.TrimSpace(b.input.DirectViewField)
	if directViewField == "" && b.input.DirectViewType == nil {
		typ := (xshape.Runtime{}).Indirect(b.input.OutputType)
		if typ != nil && typ.Kind() == reflect.Slice {
			b.input.DirectViewType = b.input.OutputType
		}
	}
	if directViewField == "" && b.input.DirectViewType == nil && b.input.Component != nil && b.input.Component.RootView != nil {
		directViewField = ResolveOutputField(b.input.Component, b.input.OutputType, ViewSlot)
	}
	// Resolve a named holder first; a declared singleton without one is direct.
	if directViewField == "" && b.input.DirectViewType == nil && b.input.Component != nil && b.input.Component.RootView != nil && b.input.Component.RootView.Cardinality == spec.CardinalityOne {
		if typ := (xshape.Runtime{}).Indirect(b.input.OutputType); typ != nil && typ.Kind() == reflect.Struct {
			b.input.DirectViewType = b.input.OutputType
		}
	}
	views, err := buildDataViews(b.input.Component, b.input.OutputType, directViewField)
	if b.input.DirectViewType != nil {
		if directViewField != "" {
			return nil, fmt.Errorf("direct view field and direct view type are mutually exclusive")
		}
		views, err = buildDirectDataViews(b.input.Component, b.input.DirectViewType)
	}
	if err != nil {
		return nil, err
	}
	view := views.root
	if err := resolveViewResources(view, b.input.Resources); err != nil {
		return nil, err
	}
	if err := resolveRelationProjections(view); err != nil {
		return nil, err
	}
	viewIndex := sqlreader.NewViewIndex(b.input.Component, view)
	selectorBindings, err := compileSelectorBindings(b.input.Component, b.input.InputType, viewIndex, b.input.Bindings)
	if err != nil {
		return nil, err
	}
	programs, err := b.compileSQLPrograms(view, b.input.Bindings, b.input.Predicate)
	if err != nil {
		return nil, err
	}
	partitioners, err := buildPartitioners(view, views.rowTypes, b.input.TypeLookup)
	if err != nil {
		return nil, err
	}
	collectorGraph, err := rcollector.Compile(view, views.rowTypes)
	if err != nil {
		return nil, err
	}
	criteriaCompilers, err := b.compileCriteria(views)
	if err != nil {
		return nil, err
	}
	codecs := map[*data.View]*rowcodec.Plan{}
	for view, rowType := range views.rowTypes {
		codec, err := rowcodec.Compile(rowcodec.Config{RowType: rowType, Columns: view.Columns, Factory: b.input.CodecFactory, LookupType: b.input.TypeLookup, Resources: b.input.Resources})
		if err != nil {
			return nil, fmt.Errorf("compile view %s column codecs: %w", view.Spec.Name, err)
		}
		if codec != nil {
			codecs[view] = codec
		}
	}
	return sqlreader.NewPlan(sqlreader.PlanConfig{
		SelectorBindings:   selectorBindings,
		ViewIndex:          viewIndex,
		OutputViewField:    directViewField,
		OutputMetricsField: ResolveOutputField(b.input.Component, b.input.OutputType, MetricsSlot),
		DirectOutput:       b.input.DirectViewType != nil,
		RootView:           view,
		Templates:          programs,
		Criteria:           criteriaCompilers,
		Partitioners:       partitioners,
		Collection:         collectorGraph,
		Codecs:             codecs,
	})
}

func (b *planCompiler) compileSQLPrograms(root *data.View, bindings []bindly.BindingSpec, predicateProgram dexec.PredicateEvaluator) (map[*data.View]sqltemplate.Evaluator, error) {
	if !b.isReaderArtifact() || root == nil {
		return nil, nil
	}
	variables, err := b.sqlTemplateVariables(bindings)
	if err != nil {
		return nil, err
	}
	parentAliases, err := b.sqlTemplateParentAliases(root)
	if err != nil {
		return nil, err
	}
	result := map[*data.View]sqltemplate.Evaluator{}
	visited := map[*data.View]bool{}
	var compile func(*data.View) error
	compile = func(view *data.View) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		if view.Spec.Source != nil {
			evaluator, err := (sqltemplate.Compiler{
				Source:           view.Spec.Source.SQL,
				InputType:        b.input.InputType,
				Variables:        variables,
				NonWindowAliases: parentAliases[view],
				Predicate:        predicateProgram,
			}).Compile()
			if err != nil {
				return fmt.Errorf("compile SQL template for view %s: %w", view.Spec.Name, err)
			}
			if evaluator != nil {
				result[view] = evaluator
			}
		}
		for _, relation := range view.Relations {
			if relation != nil && relation.Of != nil {
				if err := compile(relation.Of.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := compile(root); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func (b *planCompiler) sqlTemplateParentAliases(root *data.View) (map[*data.View][]string, error) {
	result := map[*data.View][]string{}
	parents := map[*data.View]map[*data.View]bool{}
	visited := map[*data.View]bool{}
	var collect func(*data.View)
	collect = func(parent *data.View) {
		if parent == nil || visited[parent] {
			return
		}
		visited[parent] = true
		aliases := viewTemplateAliases(parent)
		if parent == root && b.input.Component != nil {
			aliases = appendDistinctAlias(aliases, b.input.Component.Name)
		}
		for _, relation := range parent.Relations {
			if relation == nil || relation.Of == nil || relation.Of.View == nil {
				continue
			}
			child := relation.Of.View
			if parents[child] == nil {
				parents[child] = map[*data.View]bool{}
			}
			parents[child][parent] = true
			for _, alias := range aliases {
				result[child] = appendDistinctAlias(result[child], alias)
			}
			collect(child)
		}
	}
	collect(root)
	for view, owners := range parents {
		if len(owners) <= 1 || view == nil || view.Spec.Source == nil {
			continue
		}
		for _, access := range sqlmacro.NonWindowSQLAccesses(view.Spec.Source.SQL) {
			if access.Alias != "" {
				return nil, fmt.Errorf("compile SQL template for shared view %s: named parent non-window access %s is ambiguous; use $View.NonWindowSQL", view.Spec.Name, access.Raw)
			}
		}
	}
	return result, nil
}

func viewTemplateAliases(view *data.View) []string {
	if view == nil {
		return nil
	}
	result := appendDistinctAlias(nil, view.Spec.Name)
	return appendDistinctAlias(result, view.Spec.Key.Name)
}

func appendDistinctAlias(aliases []string, candidate string) []string {
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return aliases
	}
	for _, alias := range aliases {
		if alias == candidate {
			return aliases
		}
	}
	return append(aliases, candidate)
}

func (b *planCompiler) sqlTemplateVariables(bindings []bindly.BindingSpec) ([]sqltemplate.Variable, error) {
	variables := make([]sqltemplate.Variable, 0, len(bindings))
	ordered := append([]bindly.BindingSpec(nil), bindings...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Path < ordered[j].Path })
	for _, binding := range ordered {
		param, _ := binding.Extension.(*spec.Parameter)
		if param == nil || param.Name == "" {
			continue
		}
		field, ok := b.input.InputType.FieldByName(binding.Path)
		if !ok {
			return nil, fmt.Errorf("SQL template input field %s was not found", binding.Path)
		}
		variables = append(variables, sqltemplate.Variable{Name: param.Name, FieldIndex: field.Index})
	}
	for _, param := range spec.EffectiveParameters(b.input.Component.Parameters) {
		if !isBinderTemplateParam(param, b.input.InputType) {
			continue
		}
		valueType, err := b.paramType(param)
		if err != nil {
			return nil, err
		}
		if valueType == nil {
			continue
		}
		variables = append(variables, sqltemplate.Variable{
			Name:     param.Name,
			Type:     valueType,
			Key:      xhandler.ValueKey(param.Name),
			Required: param.Required != nil && *param.Required,
		})
	}
	return variables, nil
}

func isBinderTemplateParam(param *spec.Parameter, inputType reflect.Type) bool {
	if param == nil || param.EmitOutput || strings.TrimSpace(param.Name) == "" || strings.TrimSpace(param.TypeExpr) == "" {
		return false
	}
	kind := strings.ToLower(strings.TrimSpace(param.Source.Kind))
	if kind == "output" || param.IsTransportInput() {
		return false
	}
	_, found := typecatalog.FieldByName(inputType, param.Name)
	return !found
}

func (b *planCompiler) paramType(param *spec.Parameter) (reflect.Type, error) {
	options := []xreflect.Option(nil)
	if b.input.TypeLookup != nil {
		options = append(options, xreflect.WithTypeLookup(func(name string, _ ...xreflect.Option) (reflect.Type, error) {
			return b.input.TypeLookup(name)
		}))
	}
	valueType, err := xreflect.Parse(param.TypeExpr, options...)
	if err != nil {
		return nil, fmt.Errorf("resolve SQL template parameter %s type %s: %w", param.Name, param.TypeExpr, err)
	}
	return valueType, nil
}

func (b *planCompiler) isReaderArtifact() bool {
	if b.input.Component == nil {
		return false
	}
	if b.input.DirectViewType != nil {
		return true
	}
	if strings.TrimSpace(b.input.DirectViewField) != "" {
		return true
	}
	for _, param := range b.input.Component.Parameters {
		if param == nil || !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(param.Source.Name)) {
		case "view", "body", "derived", "summary":
			return true
		}
	}
	return false
}
