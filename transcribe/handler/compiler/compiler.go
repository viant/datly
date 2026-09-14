// Package compiler validates canonical component metadata and produces the
// target-neutral handler AST.
package compiler

import (
	"fmt"
	goast "go/ast"
	"go/parser"
	"strings"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/typecatalog"
)

// Request contains canonical, already-refined authority for one root plan.
type Request struct {
	Component    *spec.Component
	ViewBindings map[string]string
	Operation    plan.Operation
	Input        string
	Output       string
	Current      string
	Currents     []CurrentBinding
	Table        string
	Key          string
}

// CurrentBinding maps one canonical record-view identity to the ordinary
// component input parameter that supplies its current rows.
type CurrentBinding struct {
	ViewIdentity string
	Param        string
}

// Compiler validates canonical metadata and builds target-neutral plans.
type Compiler struct{}

type indexedCurrentBinding struct {
	name          string
	paramIdentity string
}

type compiler struct {
	currentByView map[string]indexedCurrentBinding
	currentOwner  map[string]string
	currentOrder  []string
	usedCurrent   map[string]bool
	nextOrder     int
}

// Compile builds one root write plan.
func (c *Compiler) Compile(request Request) (*plan.Plan, error) {
	return (&compiler{}).compile(request)
}

func (c *compiler) compile(request Request) (*plan.Plan, error) {
	if request.Component == nil {
		return nil, fmt.Errorf("canonical component is required")
	}
	operation := plan.Operation(strings.ToLower(strings.TrimSpace(string(request.Operation))))
	switch operation {
	case plan.OperationPost, plan.OperationPut, plan.OperationPatch:
	default:
		return nil, fmt.Errorf("unsupported write operation %q", request.Operation)
	}
	if operation == plan.OperationPost && (strings.TrimSpace(request.Current) != "" || len(request.Currents) != 0) {
		return nil, fmt.Errorf("current bindings are supported only for PATCH and PUT handler transcription")
	}
	if err := c.indexCurrentBindings(request); err != nil {
		return nil, err
	}

	input, err := c.selectInput(request.Component, request.Input)
	if err != nil {
		return nil, err
	}
	output, err := c.selectOutput(request.Component, request.Output)
	if err != nil {
		return nil, err
	}
	inputRef, err := contractRef(input, "Input")
	if err != nil {
		return nil, err
	}

	table := strings.TrimSpace(request.Table)
	if table == "" && request.Component.RootView != nil && request.Component.RootView.Source != nil {
		table = strings.TrimSpace(request.Component.RootView.Source.Table)
	}
	if table == "" && (request.Component.RootView == nil || !request.Component.RootView.Auxiliary) {
		return nil, fmt.Errorf("%s handler transcription requires an explicit table or canonical root table", operation)
	}

	root, err := c.compileRoot(request, operation, inputRef, table)
	if err != nil {
		return nil, err
	}
	for _, identity := range c.currentOrder {
		if !c.usedCurrent[identity] {
			return nil, fmt.Errorf("current-state binding for view %q is unused", identity)
		}
	}
	result := &plan.Plan{Operation: operation, Input: inputRef, Root: root}
	if output != nil {
		ref, refErr := contractRef(output, "Output")
		if refErr != nil {
			return nil, refErr
		}
		result.Output = &ref
	}
	return result, nil
}

func (c *compiler) indexCurrentBindings(request Request) error {
	c.currentByView = map[string]indexedCurrentBinding{}
	c.currentOwner = map[string]string{}
	c.currentOrder = nil
	c.usedCurrent = map[string]bool{}
	c.nextOrder = 0
	for _, binding := range request.Currents {
		identity := strings.TrimSpace(binding.ViewIdentity)
		param := strings.TrimSpace(binding.Param)
		if identity == "" || param == "" {
			return fmt.Errorf("current binding requires view identity and parameter")
		}
		if _, ok := c.currentByView[identity]; ok {
			return fmt.Errorf("current binding for view %q is duplicated", identity)
		}
		currentParam, err := c.selectCurrentParam(request.Component, param)
		if err != nil {
			return fmt.Errorf("current binding for view %q: %w", identity, err)
		}
		paramIdentity := currentParam.Identity()
		if strings.TrimSpace(request.ViewBindings[paramIdentity]) == "" {
			return fmt.Errorf("current-state input %q requires an exact canonical view binding", currentParam.Name)
		}
		c.currentByView[identity] = indexedCurrentBinding{name: currentParam.Name, paramIdentity: paramIdentity}
		c.currentOrder = append(c.currentOrder, identity)
	}
	return nil
}

func (c *compiler) compileRoot(request Request, operation plan.Operation, input plan.ContractRef, table string) (*plan.RecordPlan, error) {
	view := request.Component.RootView
	if view == nil {
		return nil, fmt.Errorf("%s handler transcription requires a canonical root view", operation)
	}
	identity, err := view.Identity()
	if err != nil {
		return nil, err
	}
	keys, err := canonicalKeys(view)
	if err != nil {
		return nil, err
	}
	if operation == plan.OperationPatch && !view.Auxiliary {
		if err = validatePatchKey(keys, request.Key); err != nil {
			return nil, err
		}
	}
	root := &plan.RecordPlan{
		Auxiliary: view.Auxiliary,
		Identity:  identity, InputPath: clonePath(input.Path), Table: table,
		Cardinality: input.Cardinality, Keys: keys,
		Write: plan.WritePolicy{ValuePath: clonePath(input.Path), Order: c.nextWriteOrder()},
	}
	switch operation {
	case plan.OperationPost:
		root.Write.Missing = plan.ActionInsert
		root.Write.Allowed = []plan.Action{plan.ActionInsert}
	case plan.OperationPut:
		if len(keys) == 0 && !root.Auxiliary {
			return nil, fmt.Errorf("put handler transcription requires at least one canonical primary key")
		}
		root.Write.Existing = plan.ActionUpdate
		root.Write.Allowed = []plan.Action{plan.ActionUpdate}
	case plan.OperationPatch:
		root.Write.Existing = plan.ActionUpdate
		root.Write.Missing = plan.ActionInsert
		root.Write.Allowed = []plan.Action{plan.ActionInsert, plan.ActionUpdate}
	}
	if operation == plan.OperationPatch || operation == plan.OperationPut {
		current := strings.TrimSpace(request.Current)
		if bound, ok := c.currentByView[identity]; ok {
			if current != "" {
				explicit, selectErr := c.selectCurrentParam(request.Component, current)
				if selectErr != nil {
					return nil, selectErr
				}
				if explicit.Identity() != bound.paramIdentity {
					return nil, fmt.Errorf("root current input %q conflicts with canonical binding %q", current, bound.name)
				}
			}
			current = bound.name
			c.usedCurrent[identity] = true
		}
		// PATCH retains its required/inferred root Current. PUT accepts only
		// explicitly selected Current, leaving direct update-only PUT unchanged.
		if operation == plan.OperationPatch && !root.Auxiliary || current != "" {
			root.Current, err = c.compileCurrent(request, current, view, root)
			if err != nil {
				return nil, err
			}
		}
	}
	if root.Auxiliary {
		root.Write = plan.WritePolicy{ValuePath: clonePath(input.Path), Order: root.Write.Order}
	} else {
		root.Sequence, err = sequencePlan(view, request.Key, operation, input.Path, nil)
		if err != nil {
			return nil, err
		}
		root.SelfRelations, err = compileSelfRelations(view)
		if err != nil {
			return nil, err
		}
	}
	stack := map[string]bool{identity: true}
	root.Relations, err = c.compileRelations(recordContext{
		request: request, operation: operation, view: view, identity: identity,
		inputPath: input.Path, destination: input.Path, stack: stack,
		auxiliary: root.Auxiliary,
	})
	if err != nil {
		return nil, err
	}
	return root, nil
}

func (c *compiler) compileCurrent(request Request, name string, recordView *spec.View, record *plan.RecordPlan) (*plan.CurrentPlan, error) {
	component, viewBindings, rootKeys := request.Component, request.ViewBindings, record.Keys
	if len(rootKeys) == 0 {
		return nil, fmt.Errorf("current-state read requires a canonical key for view %q", recordView.CanonicalName())
	}
	param, err := c.selectCurrentParam(component, name)
	if err != nil {
		return nil, err
	}
	recordIdentity, err := recordView.Identity()
	if err != nil {
		return nil, err
	}
	if !record.Auxiliary {
		if err = c.claimCurrent(param.Identity(), recordIdentity); err != nil {
			return nil, err
		}
	}
	if cardinalityOf(param) != spec.CardinalityMany {
		return nil, fmt.Errorf("current-state view input %q must have many cardinality", param.Name)
	}
	view, err := c.currentView(component, param, viewBindings[param.Identity()])
	if err != nil {
		return nil, err
	}
	currentKeys := make([]plan.KeyPart, 0, len(rootKeys))
	for _, rootKey := range rootKeys {
		currentKey, matchErr := matchCurrentKey(view, rootKey)
		if matchErr != nil {
			return nil, matchErr
		}
		currentKeys = append(currentKeys, currentKey)
	}
	fields, err := (&currentProjection{entity: recordView, current: view}).compile()
	if err != nil {
		return nil, err
	}
	identity, err := view.Identity()
	if err != nil {
		return nil, err
	}
	field := typecatalog.FieldName(param.Name)
	if field == "" {
		return nil, fmt.Errorf("current-state input %q has no canonical Go field", param.Name)
	}
	result := &plan.CurrentPlan{
		ParamIdentity: param.Identity(), ViewIdentity: identity,
		InputPath: plan.FieldPath{"Input", field}, Keys: currentKeys, Fields: fields,
	}
	if view.SelfReference != nil {
		result.Self = append(result.Self, plan.FieldRef{Field: view.SelfReference.Holder})
	}
	return result, nil
}

func (c *compiler) selectCurrentParam(component *spec.Component, name string) (*spec.Parameter, error) {
	return c.selectParam(component, name, func(param *spec.Parameter) bool {
		if param == nil || param.EmitOutput {
			return false
		}
		kind := strings.ToLower(strings.TrimSpace(param.Source.Kind))
		return kind == "view" || kind == "data_view"
	}, "current view input", true)
}

func (c *compiler) claimCurrent(paramIdentity, recordIdentity string) error {
	if owner, ok := c.currentOwner[paramIdentity]; ok && owner != recordIdentity {
		return fmt.Errorf("current-state input %q is bound to more than one writable view: %q and %q", paramIdentity, owner, recordIdentity)
	}
	c.currentOwner[paramIdentity] = recordIdentity
	return nil
}

func (c *compiler) selectInput(component *spec.Component, name string) (*spec.Parameter, error) {
	return c.selectParam(component, name, func(param *spec.Parameter) bool {
		return param != nil && !param.EmitOutput && strings.EqualFold(strings.TrimSpace(param.Source.Kind), "body")
	}, "body input", true)
}

func (c *compiler) selectOutput(component *spec.Component, name string) (*spec.Parameter, error) {
	return c.selectParam(component, name, func(param *spec.Parameter) bool {
		if param == nil {
			return false
		}
		kind := strings.ToLower(strings.TrimSpace(param.Source.Kind))
		source := strings.ToLower(strings.TrimSpace(param.Source.Name))
		if kind == "output" && (source == "status" || param.IsDerivedOutput()) {
			return false
		}
		return param.EmitOutput || kind == "output"
	}, "output", false)
}

func (c *compiler) selectParam(component *spec.Component, name string, accepts func(*spec.Parameter) bool, label string, required bool) (*spec.Parameter, error) {
	name = strings.TrimSpace(name)
	var candidates []*spec.Parameter
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if accepts(param) && (name == "" || strings.EqualFold(strings.TrimSpace(param.Name), name)) {
			candidates = append(candidates, param)
		}
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	if len(candidates) == 0 {
		if name != "" || required {
			return nil, fmt.Errorf("handler transcription %s %q was not found", label, name)
		}
		return nil, nil
	}
	if name == "" {
		return nil, fmt.Errorf("handler transcription %s is ambiguous; select one explicitly", label)
	}
	return nil, fmt.Errorf("handler transcription %s %q is ambiguous", label, name)
}

func contractRef(param *spec.Parameter, root string) (plan.ContractRef, error) {
	field := typecatalog.FieldName(param.Name)
	if field == "" {
		return plan.ContractRef{}, fmt.Errorf("handler transcription parameter %q has no canonical Go field", param.Name)
	}
	return plan.ContractRef{
		ParamIdentity: param.Identity(), Path: plan.FieldPath{root, field},
		Cardinality: cardinalityOf(param),
	}, nil
}

func cardinalityOf(param *spec.Parameter) spec.Cardinality {
	if param == nil {
		return spec.CardinalityOne
	}
	if strings.EqualFold(strings.TrimSpace(param.Cardinality), string(spec.CardinalityMany)) {
		return spec.CardinalityMany
	}
	expression := strings.TrimSpace(param.OutputTypeExpr)
	if expression == "" {
		expression = strings.TrimSpace(param.TypeExpr)
	}
	parsed, err := parser.ParseExpr(expression)
	if err == nil {
		if _, ok := parsed.(*goast.ArrayType); ok {
			return spec.CardinalityMany
		}
	}
	return spec.CardinalityOne
}

func canonicalKeys(view *spec.View) ([]plan.KeyPart, error) {
	var result []plan.KeyPart
	for _, column := range view.Columns {
		if column == nil || !column.PrimaryKey {
			continue
		}
		field := typecatalog.FieldName(column.Name)
		if field == "" {
			return nil, fmt.Errorf("view %q primary key %q has no canonical Go field", view.CanonicalName(), column.Name)
		}
		result = append(result, plan.KeyPart{Field: field, Source: strings.TrimSpace(column.Source), Type: column.EffectiveType()})
	}
	return result, nil
}

func validatePatchKey(keys []plan.KeyPart, explicit string) error {
	explicit = strings.TrimSpace(explicit)
	if len(keys) == 0 {
		return fmt.Errorf("patch handler transcription requires at least one canonical key")
	}
	if explicit == "" {
		return nil
	}
	if len(keys) == 1 && (strings.EqualFold(keys[0].Field, typecatalog.FieldName(explicit)) || strings.EqualFold(keys[0].Source, explicit)) {
		return nil
	}
	return fmt.Errorf("patch key %q is not the canonical primary key", explicit)
}

func sequencePlan(view *spec.View, explicit string, operation plan.Operation, destination, prefix plan.FieldPath) (*plan.SequencePlan, error) {
	if operation == plan.OperationPut {
		return nil, nil
	}
	explicit = strings.TrimSpace(explicit)
	var candidates []*spec.Column
	for _, column := range view.Columns {
		if column == nil {
			continue
		}
		if explicit != "" {
			if strings.EqualFold(strings.TrimSpace(column.Name), explicit) || strings.EqualFold(strings.TrimSpace(column.Source), explicit) {
				candidates = append(candidates, column)
			}
			continue
		}
		if column.PrimaryKey {
			candidates = append(candidates, column)
		}
	}
	if len(candidates) == 0 {
		if explicit != "" {
			return nil, fmt.Errorf("handler transcription key %q was not found in the canonical root view", explicit)
		}
		return nil, nil
	}
	if len(candidates) != 1 {
		if explicit != "" {
			return nil, fmt.Errorf("sequence transcription requires one canonical key, got %d", len(candidates))
		}
		return nil, nil
	}
	if !integerType(candidates[0].Type.Name) {
		if explicit != "" {
			return nil, fmt.Errorf("handler transcription key %q must be an integer", explicit)
		}
		return nil, nil
	}
	field := typecatalog.FieldName(candidates[0].Name)
	if field == "" {
		return nil, fmt.Errorf("handler transcription key %q has no canonical Go field", candidates[0].Name)
	}
	selector := append(clonePath(prefix), field)
	return &plan.SequencePlan{Destination: clonePath(destination), Selector: selector, Field: plan.FieldRef{Field: field, Source: candidates[0].Source, Type: candidates[0].EffectiveType()}}, nil
}

func integerType(name string) bool {
	switch strings.TrimPrefix(strings.TrimSpace(name), "*") {
	case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
		return true
	default:
		return false
	}
}

func clonePath(path plan.FieldPath) plan.FieldPath {
	return append(plan.FieldPath(nil), path...)
}

func (c *compiler) nextWriteOrder() int {
	order := c.nextOrder
	c.nextOrder++
	return order
}
