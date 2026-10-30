package velty

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx"
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
	marker          *compiledMarker
	applyWhenAbsent bool
	evaluator       *templateEvaluator
	handlerType     reflect.Type
	handlerArgs     []any
}

// compiledMarker is immutable input-presence metadata. Invocation reads only
// the marker value; it never reparses a struct tag or rediscovers a field.
type compiledMarker struct {
	holderIndex []int
	flagIndex   []int
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
		marker, err := compileMarker(inputType, field.Name)
		if err != nil {
			return nil, fmt.Errorf("predicate input field %q: %w", field.Name, err)
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
				marker:          marker,
				applyWhenAbsent: definition.ApplyWhenAbsent,
			}
			var err error
			if entry.handler {
				compiled.handlerType, err = resolveHandlerType(input.Lookup, definition.Args)
				if err == nil {
					compiled.handlerArgs, err = compileHandlerArgs(compiled.handlerType, definition)
				}
			} else {
				if len(definition.HandlerArgs) > 0 {
					return nil, fmt.Errorf("predicate %s does not accept handlerArgs", definition.Name)
				}
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

func compileMarker(inputType reflect.Type, fieldName string) (*compiledMarker, error) {
	holder, ok := inputType.FieldByName("Has")
	if !ok || holder.Tag.Get("setMarker") != "true" {
		return nil, nil
	}
	markerType := derefType(holder.Type)
	if markerType == nil || markerType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("setMarker Has must be a struct or pointer to struct")
	}
	flag, ok := markerType.FieldByName(fieldName)
	if !ok || flag.Type.Kind() != reflect.Bool {
		return nil, fmt.Errorf("setMarker Has needs boolean field %s", fieldName)
	}
	return &compiledMarker{
		holderIndex: append([]int(nil), holder.Index...),
		flagIndex:   append([]int(nil), flag.Index...),
	}, nil
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
	if !reflect.PointerTo(rType).Implements(reflect.TypeFor[xpredicate.Handler]()) && !reflect.PointerTo(rType).Implements(reflect.TypeFor[xpredicate.VariadicHandler]()) {
		return nil, fmt.Errorf("%v does not implement predicate.Handler", rType)
	}
	if len(args) > 1 && !reflect.PointerTo(rType).Implements(reflect.TypeFor[xpredicate.VariadicHandler]()) {
		return nil, fmt.Errorf("%v requires predicate.VariadicHandler to accept arguments", rType)
	}
	return rType, nil
}

// Criteria evaluates an existing predicate group for a database mutation.
// It uses exactly the reader's marker-aware activation, custom handler binding,
// templates, and ordered placeholders; SQLX owns the shared output fragment.
func (p *Program) Criteria(ctx context.Context, binder xhandler.Binder, group int) (*sqlx.Criteria, error) {
	result := &sqlx.Criteria{}
	value, err := p.NewContext(ctx, binder, func(args ...any) { result.Placeholders = append(result.Placeholders, args...) })
	if err != nil {
		return nil, err
	}
	result.Expression, err = value.(*Context).Expand(group)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(result.Expression) == "" {
		return nil, nil
	}
	return result, nil
}

// Arguments are validated and detached once during compilation. Only immutable
// scalar literals are supported, so handlers cannot mutate shared configuration.
func compileHandlerArgs(typ reflect.Type, definition *spec.Predicate) ([]any, error) {
	args := make([]any, 0, len(definition.Args)-1+len(definition.HandlerArgs))
	for _, arg := range definition.Args[1:] {
		args = append(args, arg)
	}
	args = append(args, definition.HandlerArgs...)
	if len(args) > 0 && !reflect.PointerTo(typ).Implements(reflect.TypeFor[xpredicate.VariadicHandler]()) {
		return nil, fmt.Errorf("%v requires predicate.VariadicHandler to accept arguments", typ)
	}
	for _, arg := range args {
		if err := spec.ValidateHandlerArgument(arg); err != nil {
			return nil, err
		}
	}
	return args, nil
}
