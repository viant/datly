package writer

import (
	"context"
	"fmt"
	"reflect"

	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

func hasQueueContract(record *Record) bool {
	if record == nil {
		return false
	}
	if record.QueueContract != "" {
		return true
	}
	for _, relation := range record.Relations {
		if hasQueueContract(relation.Child) {
			return true
		}
	}
	return false
}

func hasRetainedWriterGuards(record *Record) bool {
	return hasWriterActionPolicy(record) || hasQueueContract(record) || hasAfterQueueInput(record) || hasAfterValidateInput(record)
}

func validateQueueContracts(record *Record, operation string) error {
	if record == nil {
		return nil
	}
	if record.QueueContract != "" {
		if record.QueueContract != "source-row" {
			return fmt.Errorf("queue_contract requires source-row; source-slice authoring is not yet available")
		}
		if record.Auxiliary || record.Table == "" || (operation != "post" && operation != "patch") || record.ConcurrencyToken != nil || record.MutationPredicateGroup != nil {
			return fmt.Errorf("queue_contract source-row requires POST/PATCH physical role without matched/criteria options")
		}
	}
	for _, relation := range record.Relations {
		if err := validateQueueContracts(relation.Child, operation); err != nil {
			return err
		}
	}
	return nil
}

func (p *Program) preflightQueueContracts(ctx context.Context, binder xhandler.Binder) error {
	if !hasQueueContract(p.metadata.Root) {
		return nil
	}
	service, err := lookup[xhandler.DML](ctx, binder, xhandler.DMLKey)
	if err != nil {
		return err
	}
	if _, ok := service.(rhandler.QueueContractDML); !ok {
		return fmt.Errorf("native queue_contract capability is unavailable")
	}
	if _, ok := service.(interface{ ValidateExecutionGuards(context.Context) error }); !ok {
		return fmt.Errorf("queue contract requires retained execution guards")
	}
	for _, action := range p.actions.Rows {
		frame := p.actionFrame(action)
		if frame == nil {
			return fmt.Errorf("queue contract action has no captured frame")
		}
		if frame.Record.QueueContract != "" && action.Kind != xhandler.WriteInsert && action.Kind != xhandler.WriteDelete {
			return fmt.Errorf("queue_contract source-row does not support %s", action.Kind)
		}
	}
	return nil
}

func (p *Program) admitSourceRow(dml xhandler.DML, action *Action, frame *Frame) error {
	native, ok := dml.(rhandler.QueueContractDML)
	if !ok {
		return fmt.Errorf("native queue_contract capability is unavailable")
	}
	if action.Kind == xhandler.WriteInsert && frame.Record.Sequence != nil {
		value := frame.Entity.Elem().FieldByIndex(frame.Record.Sequence.Index)
		if !linkValueResolved(value) {
			return fmt.Errorf("queue_contract native allocation is unresolved for %s", frame.Location)
		}
	}
	seal, err := p.captureQueueSlots(frame)
	if err != nil {
		return err
	}
	if err = seal.validate(reflect.ValueOf(p.input)); err != nil {
		return err
	}
	// Publish retained holder evidence before any operation becomes visible.
	// Failed append keeps the seal: execution failure retires this captured attempt.
	p.guardMu.Lock()
	p.queueSlots = append(p.queueSlots, seal)
	p.guardMu.Unlock()
	switch action.Kind {
	case xhandler.WriteInsert:
		err = native.InsertWithQueueContract(frame.Record.Table, action.Entity.Interface(), rhandler.SourceRow)
	case xhandler.WriteDelete:
		err = native.DeleteWithQueueContract(frame.Record.Table, action.Entity.Interface(), rhandler.SourceRow)
	default:
		err = fmt.Errorf("queue_contract unsupported action %s", action.Kind)
	}
	if err != nil {
		return err
	}
	return nil
}

// Descriptors own copies of the paths/positions and holder identity evidence.
// They never consult later mutable Frame/Action fields or compare public scalars.
type queueSlotStep struct {
	field         []int
	indexed       bool
	position      int
	holderType    reflect.Type
	holderPointer uintptr
	elements      []uintptr
	entityType    reflect.Type
	entityPointer uintptr
}
type queueSlotSeal struct {
	location string
	steps    []queueSlotStep
}

func (p *Program) captureQueueSlots(frame *Frame) (queueSlotSeal, error) {
	result := queueSlotSeal{location: frame.Location}
	var ancestors []*Frame
	for owner := frame; owner != nil; owner = owner.Parent {
		ancestors = append([]*Frame{owner}, ancestors...)
	}
	v := reflect.ValueOf(p.input)
	for _, owner := range ancestors {
		if !owner.holderTracked {
			return result, fmt.Errorf("queue contract requires captured holder at %s", owner.Location)
		}
		field := []int{p.metadata.InputField}
		if owner.Parent != nil {
			relation := relationFor(owner.Parent.Record, owner.Record)
			if relation == nil {
				return result, fmt.Errorf("queue contract has no captured relation")
			}
			field = append([]int(nil), relation.Field...)
		}
		if v.Kind() != reflect.Pointer || v.IsNil() {
			return result, fmt.Errorf("queue contract ancestor is nil")
		}
		holder := v.Elem().FieldByIndex(field)
		step := queueSlotStep{field: field, indexed: owner.holderIndexed, position: owner.holderPosition, holderType: holder.Type(), entityType: owner.Entity.Type(), entityPointer: owner.Entity.Pointer()}
		if step.indexed {
			if holder.Kind() != reflect.Slice {
				return result, fmt.Errorf("queue contract collection holder is invalid")
			}
			step.holderPointer = holder.Pointer()
			for i := 0; i < holder.Len(); i++ {
				element := holder.Index(i)
				if element.Kind() != reflect.Pointer {
					return result, fmt.Errorf("queue contract collection requires pointer slots")
				}
				step.elements = append(step.elements, element.Pointer())
			}
			if step.position < 0 || step.position >= holder.Len() {
				return result, fmt.Errorf("queue contract slot is absent")
			}
			v = holder.Index(step.position)
		} else {
			v = holder
		}
		result.steps = append(result.steps, step)
	}
	return result, nil
}

func (s queueSlotSeal) validate(input reflect.Value) error {
	v := input
	for _, step := range s.steps {
		if !v.IsValid() || v.Kind() != reflect.Pointer || v.IsNil() {
			return fmt.Errorf("queue slot ancestor changed at %s", s.location)
		}
		holder := v.Elem().FieldByIndex(step.field)
		if holder.Type() != step.holderType {
			return fmt.Errorf("queue slot holder changed at %s", s.location)
		}
		if step.indexed {
			if holder.Pointer() != step.holderPointer || holder.Len() != len(step.elements) {
				return fmt.Errorf("queue slot collection changed at %s", s.location)
			}
			for i, pointer := range step.elements {
				if holder.Index(i).Pointer() != pointer {
					return fmt.Errorf("queue slot member changed at %s", s.location)
				}
			}
			v = holder.Index(step.position)
		} else {
			v = holder
		}
		if v.Type() != step.entityType || v.Kind() != reflect.Pointer || v.IsNil() || v.Pointer() != step.entityPointer {
			return fmt.Errorf("queue slot row changed at %s", s.location)
		}
	}
	return nil
}

func (p *Program) validateQueueSlots() error {
	p.guardMu.Lock()
	seals := append([]queueSlotSeal(nil), p.queueSlots...)
	p.guardMu.Unlock()
	for _, seal := range seals {
		if err := seal.validate(reflect.ValueOf(p.input)); err != nil {
			return err
		}
	}
	return nil
}

// Called after native queue hooks/observers, outside journal locks. Execution's
// own guard path calls its private locked helper, never this public method.
func (p *Program) validateQueuedContractState(ctx context.Context, binder xhandler.Binder) error {
	if !hasQueueContract(p.metadata.Root) && !hasAfterQueueInput(p.metadata.Root) {
		return nil
	}
	if err := p.validateQueueSlots(); err != nil {
		return err
	}
	service, err := lookup[xhandler.DML](ctx, binder, xhandler.DMLKey)
	if err != nil {
		return err
	}
	guarded, ok := service.(interface{ ValidateExecutionGuards(context.Context) error })
	if !ok {
		return fmt.Errorf("queue contract requires retained execution guards")
	}
	return guarded.ValidateExecutionGuards(ctx)
}
