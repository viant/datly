package velty

import (
	"context"
	"fmt"
	xhandler "github.com/viant/xdatly/handler"
	xpredicate "github.com/viant/xdatly/predicate"
	"reflect"
	"strings"
)

type Context struct {
	program  *Program
	ctx      context.Context
	input    reflect.Value
	append   func(...any)
	binder   xhandler.Binder
	handlers map[int]xpredicate.Handler
}

func newContext(program *Program, ctx context.Context, input any, appendArgs func(...any)) *Context {
	return &Context{program: program, ctx: ctx, input: dereferencePredicateValue(reflect.ValueOf(input)), append: appendArgs}
}

func (c *Context) Expand(group int) (string, error) {
	return c.expandGroup(group, "AND")
}

func (c *Context) ExpandWith(group int, operator string) (string, error) {
	return c.expandGroup(group, operator)
}

func (c *Context) FilterGroup(group int, operator string) (string, error) {
	return c.expandGroup(group, operator)
}

func (c *Context) Builder() *Builder {
	return &Builder{}
}

func (c *Context) expandGroup(group int, operator string) (string, error) {
	if c == nil || c.program == nil {
		return "", nil
	}
	fragments := make([]string, 0)
	for index, predicate := range c.program.predicates {
		if predicate.group != group {
			continue
		}
		value, present, err := predicateValue(c.input, predicate.fieldIndex, predicate.fieldName)
		if err != nil {
			return "", err
		}
		if !predicate.applyWhenAbsent && !present {
			continue
		}
		var fragment string
		var args []any
		if predicate.handlerType != nil {
			handler := c.handlers[index]
			if handler == nil {
				handler = reflect.New(predicate.handlerType).Interface().(xpredicate.Handler)
				if c.binder == nil {
					return "", fmt.Errorf("custom predicate requires an invocation binder")
				}
				if err := c.binder.Bind(c.ctx, handler); err != nil {
					return "", fmt.Errorf("bind predicate %v: %w", predicate.handlerType, err)
				}
				if c.handlers == nil {
					c.handlers = map[int]xpredicate.Handler{}
				}
				c.handlers[index] = handler
			}
			criteria, err := handler.Compute(c.ctx, value)
			if err != nil {
				return "", err
			}
			if criteria != nil {
				fragment = criteria.Expression
				args = criteria.Placeholders
			}
		} else {
			fragment, args, err = predicate.evaluator.evaluate(c.ctx, value, present)
			if err != nil {
				return "", err
			}
		}
		fragment = strings.TrimSpace(fragment)
		if fragment == "" {
			continue
		}
		fragments = append(fragments, fragment)
		if c.append != nil {
			c.append(args...)
		}
	}
	return joinFragments(fragments, operator), nil
}

func predicateValue(input reflect.Value, index []int, fieldName string) (any, bool, error) {
	if !input.IsValid() || input.Kind() != reflect.Struct {
		return nil, false, fmt.Errorf("predicate input must be a struct")
	}
	value, err := input.FieldByIndexErr(index)
	if err != nil {
		return nil, false, fmt.Errorf("read predicate field %s: %w", fieldName, err)
	}
	actual := value.Interface()
	if present, authoritative := markerPresence(input, fieldName); authoritative {
		return actual, present, nil
	}
	return actual, !isUnsetPredicateValue(actual), nil
}

func markerPresence(input reflect.Value, fieldName string) (bool, bool) {
	markerField, ok := input.Type().FieldByName("Has")
	if !ok || markerField.Tag.Get("setMarker") != "true" {
		return false, false
	}
	marker := input.FieldByIndex(markerField.Index)
	marker = dereferencePredicateValue(marker)
	if !marker.IsValid() {
		return false, true
	}
	if marker.Kind() != reflect.Struct {
		return false, false
	}
	flag := marker.FieldByName(fieldName)
	if !flag.IsValid() || flag.Kind() != reflect.Bool {
		return false, false
	}
	return flag.Bool(), true
}

func dereferencePredicateValue(value reflect.Value) reflect.Value {
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}
	return value
}

func joinFragments(fragments []string, operator string) string {
	if operator = strings.TrimSpace(operator); operator == "" {
		operator = "AND"
	}
	parts := make([]string, 0, len(fragments))
	for _, fragment := range fragments {
		if fragment = strings.TrimSpace(fragment); fragment != "" {
			parts = append(parts, "( "+fragment+" )")
		}
	}
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return "( " + strings.Join(parts, " "+operator+" ") + " )"
}
