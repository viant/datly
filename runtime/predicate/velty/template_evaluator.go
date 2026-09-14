package velty

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/datly/sql/fragment"
	"github.com/viant/velty"
	"github.com/viant/velty/est"
	xpredicate "github.com/viant/xdatly/predicate"
)

type templateEvaluator struct {
	exec      *est.Execution
	newState  func() *est.State
	argNames  []string
	argValues []string
}

func (e *templateEvaluator) evaluate(ctx context.Context, value any, hasValue bool) (string, []any, error) {
	state := e.newState()
	bindings := &fragment.Bindings{}
	criteria := fragment.New(bindings).WithDialect(fragment.Dialect(ctx))
	if err := state.SetValue("criteria", criteria); err != nil {
		return "", nil, err
	}
	if err := state.SetValue("FilterValue", value); err != nil {
		return "", nil, err
	}
	if err := state.SetValue("HasFilterValue", hasValue); err != nil {
		return "", nil, err
	}
	for i, name := range e.argNames {
		if err := state.SetValue(name, e.argValues[i]); err != nil {
			return "", nil, err
		}
	}
	if err := e.exec.Exec(state); err != nil {
		return "", nil, err
	}
	return state.Buffer.String(), bindings.Args(), nil
}

func newTemplateEvaluator(template *xpredicate.Template, valueType reflect.Type, argValues []string) (*templateEvaluator, error) {
	if template == nil {
		return nil, fmt.Errorf("predicate template is required")
	}
	planner := velty.New(velty.BufferSize(len(template.Source) + 64))
	if err := planner.DefineVariable("criteria", fragment.New(nil)); err != nil {
		return nil, err
	}
	if valueType == nil {
		valueType = reflect.TypeOf("")
	}
	if err := planner.DefineVariable("FilterValue", valueType); err != nil {
		return nil, err
	}
	if err := planner.DefineVariable("HasFilterValue", false); err != nil {
		return nil, err
	}
	argNames := make([]string, 0, len(template.Args))
	resolved := make([]string, len(template.Args))
	for i, arg := range template.Args {
		if arg == nil {
			return nil, fmt.Errorf("predicate template %q has nil argument", template.Name)
		}
		if err := planner.DefineVariable(arg.Name, ""); err != nil {
			return nil, err
		}
		argNames = append(argNames, arg.Name)
		if arg.Position < len(argValues) {
			resolved[i] = argValues[arg.Position]
		}
	}
	exec, newState, err := planner.Compile([]byte(template.Source))
	if err != nil {
		return nil, err
	}
	return &templateEvaluator{
		exec:      exec,
		newState:  newState,
		argNames:  argNames,
		argValues: resolved,
	}, nil
}
