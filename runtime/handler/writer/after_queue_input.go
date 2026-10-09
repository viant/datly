package writer

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	xhandler "github.com/viant/xdatly/handler"
)

func hasAfterQueueInput(root *Record) bool {
	if root == nil || root.HookType == nil {
		return false
	}
	_, ok := reflect.PointerTo(root.HookType).MethodByName("AfterQueueInput")
	return ok
}

func validateAfterQueueInputHooks(root *Record, inputType, outputType reflect.Type) error {
	seen := map[*Record]bool{}
	var visit func(*Record) error
	visit = func(record *Record) error {
		if record == nil || seen[record] {
			return nil
		}
		seen[record] = true
		if record.HookType != nil {
			method := reflect.New(record.HookType).MethodByName("AfterQueueInput")
			if method.IsValid() {
				if record != root || root.Auxiliary && len(root.Relations) == 0 {
					return fmt.Errorf("AfterQueueInput is only supported on the writer root")
				}
				typ := method.Type()
				if typ.IsVariadic() || typ.NumIn() != 3 || typ.NumOut() != 1 || typ.In(0) != reflect.TypeFor[context.Context]() || typ.In(1) != reflect.PointerTo(inputType) || typ.In(2) != reflect.PointerTo(outputType) || typ.Out(0) != reflect.TypeFor[error]() {
					return fmt.Errorf("AfterQueueInput requires canonical context.Context, *Input, *Output and error")
				}
			}
		}
		for _, relation := range record.Relations {
			if err := visit(relation.Child); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root)
}

func (p *Program) afterQueueInputWasStarted() bool {
	p.guardMu.Lock()
	defer p.guardMu.Unlock()
	return p.afterQueueInputStarted
}

// Preserve the actual queued graph and evidence, while allowing independent
// response metadata. The native output body is populated after this boundary;
// mutations through any existing output alias are covered by the input graph.
func (p *Program) afterQueueInputState() (string, error) {
	return p.afterQueueInputStateForActions(p.actions.Rows)
}

// Explicit action spans preserve immutable predecessor evidence while the native
// journal grows. This never swaps the Program journal during validation.
func (p *Program) afterQueueInputStateForActions(actions []*Action) (string, error) {
	input := reflect.ValueOf(p.input).Elem()
	values := []reflect.Value{input.Field(p.metadata.InputField), reflect.ValueOf(len(p.frames.Rows)), reflect.ValueOf(len(actions))}
	seen := map[*Record]bool{}
	var currents func(*Record)
	currents = func(record *Record) {
		if record == nil || seen[record] {
			return
		}
		seen[record] = true
		if record.CurrentField >= 0 {
			values = append(values, input.Field(record.CurrentField))
		}
		values = append(values, reflect.ValueOf(p.previousFields[record]))
		for _, relation := range record.Relations {
			currents(relation.Child)
		}
	}
	currents(p.metadata.Root)
	for _, frame := range p.frames.Rows {
		values = append(values, reflect.ValueOf(reflect.ValueOf(frame).Pointer()))
		if frame == nil {
			continue
		}
		values = append(values, frame.Entity, frame.Previous, frame.ExpectedToken, reflect.ValueOf(frame.Action), reflect.ValueOf(reflect.ValueOf(frame.Parent).Pointer()))
		if frame.Previous.IsValid() && !frame.Previous.IsNil() {
			values = append(values, reflect.ValueOf(p.typeFields[frame.Previous.Elem().Type()]))
		}
		if frame.Original != nil {
			values = append(values, reflect.ValueOf(frame.Original.Available()))
			for _, field := range frame.Record.Fields {
				values = append(values, reflect.ValueOf(frame.Original.Has(field.Name)))
			}
		}
	}
	for _, action := range actions {
		values = append(values, reflect.ValueOf(reflect.ValueOf(action).Pointer()))
		if action != nil {
			values = append(values, reflect.ValueOf(action.Kind), action.Entity, reflect.ValueOf(reflect.ValueOf(action.frame).Pointer()))
		}
	}
	return immutableValues(values)
}

func (p *Program) validateAfterQueueInputState() error {
	p.guardMu.Lock()
	before := p.afterQueueInputSnapshot
	p.guardMu.Unlock()
	if before == "" {
		return nil
	}
	after, err := p.afterQueueInputState()
	if err != nil {
		return err
	}
	if before != after {
		return fmt.Errorf("AfterQueueInput changed queued body, presence, identity, associations or native evidence")
	}
	return nil
}

func (p *Program) callAfterQueueInput(ctx context.Context, binder xhandler.Binder) (err error) {
	if !hasAfterQueueInput(p.metadata.Root) {
		return nil
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = p.validateQueuedContractState(ctx, binder); err != nil {
		return err
	}
	before, err := p.afterQueueInputState()
	if err != nil {
		return err
	}
	p.guardMu.Lock()
	if p.afterQueueInputStarted {
		p.guardMu.Unlock()
		return fmt.Errorf("AfterQueueInput execution already attempted; fresh capture required")
	}
	p.afterQueueInputStarted, p.afterQueueInputSnapshot = true, before
	p.guardMu.Unlock()
	defer func() {
		err = errors.Join(err, ctx.Err(), p.validateAfterQueueInputState(), p.validateAuxiliaryTopology(), p.validateActionPolicyFacts(), p.validateActionPolicyActions(), p.validateQueuedContractState(ctx, binder))
	}()
	results := p.hook.MethodByName("AfterQueueInput").Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(p.input), reflect.ValueOf(p.output)})
	return methodError("AfterQueueInput", results)
}
