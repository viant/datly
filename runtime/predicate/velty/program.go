package velty

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	xhandler "github.com/viant/xdatly/handler"
	xpredicate "github.com/viant/xdatly/predicate"
)

const VariableName = "predicate"

type LookupType func(name string) (reflect.Type, error)

type Program struct {
	predicates []compiledPredicate
}

type CompileInput struct {
	Component *spec.Component
	InputType reflect.Type
	Bindings  []bindly.BindingSpec
	Lookup    LookupType
}

func (*Program) ContextType() reflect.Type {
	return reflect.TypeOf(Context{})
}

func (p *Program) NewContext(ctx context.Context, binder xhandler.Binder, appendArgs func(...any)) (any, error) {
	if binder == nil {
		return nil, fmt.Errorf("predicate input binder is required")
	}
	input, ok, err := binder.Lookup(ctx, xhandler.InputKey)
	if err != nil {
		return nil, fmt.Errorf("resolve predicate input: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("predicate input is not available")
	}
	result := newContext(p, ctx, input, appendArgs)
	result.binder = binder
	return result, nil
}

type compiledPredicate struct {
	group           int
	fieldName       string
	fieldIndex      []int
	applyWhenAbsent bool
	evaluator       *templateEvaluator
	handlerType     reflect.Type
}

func Compile(input CompileInput) (*Program, error) {
	program := &Program{}
	component := input.Component
	if !hasPredicateMetadata(component) {
		return program, nil
	}
	inputType := derefType(input.InputType)
	if inputType == nil || inputType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("predicate input must be a struct, got %v", inputType)
	}
	registry := predicateRegistry()
	for _, param := range component.Parameters {
		if param == nil || len(param.Predicates) == 0 {
			continue
		}
		field, ok := predicateField(inputType, param, input.Bindings)
		if !ok {
			return nil, fmt.Errorf("predicate input field %q was not found on %v", param.Name, inputType)
		}
		for _, definition := range param.Predicates {
			if definition == nil {
				continue
			}
			entry, ok := registry[strings.ToLower(strings.TrimSpace(definition.Name))]
			if !ok {
				return nil, fmt.Errorf("predicate %q is not registered", definition.Name)
			}
			compiled := compiledPredicate{
				group:           definition.Group,
				fieldName:       field.Name,
				fieldIndex:      append([]int(nil), field.Index...),
				applyWhenAbsent: definition.ApplyWhenAbsent,
			}
			var err error
			if entry.handler {
				compiled.handlerType, err = resolveHandlerType(input.Lookup, definition.Args)
			} else {
				compiled.evaluator, err = newTemplateEvaluator(entry.template, field.Type, definition.Args)
			}
			if err != nil {
				return nil, fmt.Errorf("compile predicate %s for %s: %w", definition.Name, param.Name, err)
			}
			program.predicates = append(program.predicates, compiled)
		}
	}
	return program, nil
}

func predicateField(inputType reflect.Type, param *spec.Parameter, bindings []bindly.BindingSpec) (reflect.StructField, bool) {
	for _, binding := range bindings {
		if binding.Extension == param {
			return typecatalog.FieldByName(inputType, binding.Path)
		}
	}
	return typecatalog.FieldByName(inputType, param.Name)
}

func hasPredicateMetadata(component *spec.Component) bool {
	if component == nil {
		return false
	}
	for _, param := range component.Parameters {
		if param != nil && len(param.Predicates) > 0 {
			return true
		}
	}
	return false
}

func resolveHandlerType(lookup LookupType, args []string) (reflect.Type, error) {
	if len(args) == 0 || strings.TrimSpace(args[0]) == "" {
		return nil, fmt.Errorf("handler predicate requires an absolute type name")
	}
	if len(args) != 1 {
		return nil, fmt.Errorf("handler predicate accepts one type argument; bind configuration through predicate fields")
	}
	if lookup == nil {
		return nil, fmt.Errorf("handler predicate %q requires type lookup", args[0])
	}
	rType, err := lookup(args[0])
	if err != nil {
		return nil, err
	}
	if rType == nil {
		return nil, fmt.Errorf("handler predicate type %q was not found", args[0])
	}
	rType = derefType(rType)
	if !reflect.PointerTo(rType).Implements(reflect.TypeOf((*xpredicate.Handler)(nil)).Elem()) {
		return nil, fmt.Errorf("%v does not implement predicate.Handler", rType)
	}
	return rType, nil
}
